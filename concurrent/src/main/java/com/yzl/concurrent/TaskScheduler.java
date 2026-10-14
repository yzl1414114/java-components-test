package com.yzl.concurrent;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高效任务执行器：
 * <ul>
 * <li>周期性从远端 HTTP 服务拉取任务，提交工作线程并发执行（任务有大有小，一般 1~5s 完成）</li>
 * <li>资源不够时，任务启动执行后 50ms 内返回 {@link ErrorCode#ERROR_RESOURCE_FULL}，
 *     该任务进入缓存队列，暂停拉取新任务</li>
 * <li>任一任务完成后立即重试缓存任务；或暂停满 probeIntervalMs 后试探执行一次</li>
 * </ul>
 */
public class TaskScheduler {

    private final TaskClient client;
    private final TaskRunner runner;
    private final ExecutorService workers;
    private final int workerThreads;
    private final int fetchBatch;
    private final long probeIntervalMs;
    private final long pollIntervalMs;
    private final long startMs = System.currentTimeMillis();

    /** 资源不足被缓存、等待重试的任务 */
    private final Deque<Task> pending = new ConcurrentLinkedDeque<>();
    /** 调度循环的唤醒锁：任一任务成功完成（资源释放）时唤醒 */
    private final Object wakeLock = new Object();
    private boolean woken;
    /** 空闲状态等待锁：供 awaitIdle 使用 */
    private final Object stateLock = new Object();

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean fetchExhausted = new AtomicBoolean(false);
    private final AtomicInteger inFlight = new AtomicInteger();

    private final AtomicLong fetchedCount = new AtomicLong();
    private final AtomicLong succeededCount = new AtomicLong();
    private final AtomicLong resourceRejectedCount = new AtomicLong();
    private final AtomicLong cachedCount = new AtomicLong();
    private final AtomicLong probeCount = new AtomicLong();
    private final ConcurrentMap<String, TaskResult> results = new ConcurrentHashMap<>();

    private volatile boolean stopped;
    private Thread loopThread;

    public TaskScheduler(TaskClient client, TaskRunner runner, int workerThreads,
                         int fetchBatch, long probeIntervalMs, long pollIntervalMs) {
        this.client = client;
        this.runner = runner;
        this.workerThreads = workerThreads;
        this.fetchBatch = fetchBatch;
        this.probeIntervalMs = probeIntervalMs;
        this.pollIntervalMs = pollIntervalMs;
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "task-worker-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        this.workers = Executors.newFixedThreadPool(workerThreads, tf);
    }

    public void start() {
        loopThread = new Thread(this::loop, "task-scheduler");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    /**
     * 等待远端任务全部取完、缓存清空且没有在途任务
     *
     * @return true 表示已空闲
     */
    public boolean awaitIdle(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (stateLock) {
            while (!isIdle()) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    return false;
                }
                stateLock.wait(Math.min(remain, 200));
            }
            return true;
        }
    }

    public void close() {
        stopped = true;
        wake();
        signalState();
        workers.shutdownNow();
        if (loopThread != null) {
            loopThread.interrupt();
            try {
                loopThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public TaskResult getResult(String taskId) {
        return results.get(taskId);
    }

    public long getFetchedCount() {
        return fetchedCount.get();
    }

    public long getSucceededCount() {
        return succeededCount.get();
    }

    public long getResourceRejectedCount() {
        return resourceRejectedCount.get();
    }

    public long getCachedCount() {
        return cachedCount.get();
    }

    public long getProbeCount() {
        return probeCount.get();
    }

    private void loop() {
        while (!stopped) {
            if (paused.get()) {
                boolean byCompletion = waitForWake(probeIntervalMs);
                if (stopped) {
                    break;
                }
                if (!byCompletion) {
                    probeCount.incrementAndGet();
                    log("暂停超时，试探执行缓存任务");
                }
                drainPending();
            } else {
                drainPending();
                fetchAndDispatch();
                if (stopped) {
                    break;
                }
                waitForWake(pollIntervalMs);
            }
        }
    }

    private void fetchAndDispatch() {
        List<Task> batch;
        try {
            batch = client.fetch(fetchBatch);
        } catch (IOException e) {
            log("拉取任务失败: " + e.getMessage());
            return;
        }
        if (batch.isEmpty()) {
            if (fetchExhausted.compareAndSet(false, true)) {
                log("远端任务已全部取完");
            }
            signalState();
            return;
        }
        fetchExhausted.set(false);
        fetchedCount.addAndGet(batch.size());
        log("拉取任务 " + batch.size() + " 个: " + ids(batch));
        for (Task t : batch) {
            dispatch(t);
        }
    }

    private void dispatch(Task task) {
        if (inFlight.get() >= workerThreads) {
            cache(task, "工作线程已满");
            paused.set(true);
            return;
        }
        inFlight.incrementAndGet();
        log("开始执行 " + task);
        workers.submit(new Runnable() {
            @Override
            public void run() {
                TaskResult result;
                try {
                    result = runner.run(task);
                } catch (Throwable e) {
                    result = TaskResult.fail(task, ErrorCode.ERROR_INTERRUPTED, 0);
                }
                onDone(task, result);
            }
        });
    }

    private void onDone(Task task, TaskResult result) {
        inFlight.decrementAndGet();
        results.put(task.getId(), result);
        if (result.isResourceFull()) {
            resourceRejectedCount.incrementAndGet();
            cache(task, "资源不足, " + result.getElapsedMs() + "ms 返回错误码 "
                    + result.getCode());
            // 暂停拉取与执行；资源未释放，不唤醒，等待运行中的任务完成或试探超时
            paused.set(true);
        } else {
            if (result.isSuccess()) {
                succeededCount.incrementAndGet();
            }
            log((result.isSuccess() ? "执行成功 " : "执行失败 ") + task.getId()
                    + ", code=" + result.getCode() + ", elapsed=" + result.getElapsedMs() + "ms"
                    + ", inFlight=" + inFlight.get());
            // 有资源释放，唤醒调度循环重试缓存任务
            wake();
        }
        signalState();
    }

    private void cache(Task task, String reason) {
        pending.addLast(task);
        cachedCount.incrementAndGet();
        log("缓存任务 " + task.getId() + "（" + reason + "），缓存数=" + pending.size());
    }

    /** 重试缓存任务；缓存为空且处于暂停态时恢复拉取 */
    private void drainPending() {
        if (pending.isEmpty()) {
            if (paused.compareAndSet(true, false)) {
                log("缓存任务已清空，恢复拉取");
                signalState();
            }
            return;
        }
        int n = 0;
        Task t;
        while ((t = pending.pollFirst()) != null) {
            n++;
            dispatch(t);
        }
        if (n > 0) {
            log("重试缓存任务 " + n + " 个");
        }
    }

    /**
     * 等待唤醒信号，最多 timeoutMs
     *
     * @return true 表示被任务完成唤醒，false 表示超时（试探）
     */
    private boolean waitForWake(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (wakeLock) {
            while (!woken && !stopped) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    break;
                }
                try {
                    wakeLock.wait(remain);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            boolean wasWoken = woken;
            woken = false;
            return wasWoken;
        }
    }

    private void wake() {
        synchronized (wakeLock) {
            woken = true;
            wakeLock.notifyAll();
        }
    }

    private boolean isIdle() {
        return fetchExhausted.get() && pending.isEmpty() && inFlight.get() == 0;
    }

    private void signalState() {
        synchronized (stateLock) {
            stateLock.notifyAll();
        }
    }

    private static String ids(List<Task> tasks) {
        StringBuilder sb = new StringBuilder();
        for (Task t : tasks) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(t.getId()).append('(').append(t.getDurationMs()).append("ms)");
        }
        return sb.toString();
    }

    private void log(String msg) {
        System.out.printf("[%6dms][%-14s] %s%n",
                System.currentTimeMillis() - startMs, Thread.currentThread().getName(), msg);
    }
}
