package com.yzl.consume;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 调度器（v2 思路）：
 * <ol>
 * <li>从 {@link TaskQueue} 取任务（优先重试队列）提交线程池；两队列都为空时通知 {@link TaskFetcher} 抓取并阻塞等待</li>
 * <li>提交前检查 {@link #lastResult}：资源池满则暂停，最长 resourceFullPauseMs(1s)，
 *     或被非资源满执行结果提前唤醒</li>
 * <li>每次提交后 sleep submitIntervalMs(10ms) 节流，避免资源池满快速失败过多</li>
 * <li>资源池满快速失败的任务放回 {@link TaskQueue} 重试队列，优先取出重新提交；并发达到 maxConcurrent 或线程池满时同样放回重试队列并暂停等待，任务不丢失</li>
 * </ol>
 */
public class TaskScheduler {

    private final TaskFetcher fetcher;
    private final TaskQueue taskQueue;
    private final TaskRunner runner;
    private final ThreadPoolExecutor pool;
    private final int maxConcurrent;
    private final long resourceFullPauseMs;
    private final long submitIntervalMs;
    private final long startMs = System.currentTimeMillis();

    /**
     * TaskRunner 每次执行完成后写入的最近一次执行结果
     */
    private volatile TaskResult lastResult;

    /**
     * 资源池暂停唤醒信号：容量 1，非资源满任务完成时 offer，唤醒暂停等待（重复 offer 自动合并）
     */
    private final BlockingQueue<Boolean> pauseSignal = new ArrayBlockingQueue<>(1);
    private final AtomicLong submittedCount = new AtomicLong();
    private final AtomicLong succeededCount = new AtomicLong();
    private final AtomicLong resourceRejectedCount = new AtomicLong();
    private final AtomicLong pauseCount = new AtomicLong();
    private final ConcurrentMap<String, TaskResult> results = new ConcurrentHashMap<>();

    private volatile boolean stopped;
    private Thread loopThread;

    public TaskScheduler(TaskFetcher fetcher, TaskRunner runner, int maxConcurrent,
                         long resourceFullPauseMs, long submitIntervalMs) {
        this.fetcher = fetcher;
        this.taskQueue = fetcher.getTaskQueue();
        this.runner = runner;
        this.maxConcurrent = maxConcurrent;
        this.resourceFullPauseMs = resourceFullPauseMs;
        this.submitIntervalMs = submitIntervalMs;
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "task-pool-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };

        // 线程池比并发闸门多 5 个线程余量，避免提交瞬间被池拒绝
        int poolSize = maxConcurrent + 5;
        this.pool = new ThreadPoolExecutor(Math.min(8, poolSize),
                poolSize,
                30 * 1000L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),  // 无缓冲队列
                tf,
                new ThreadPoolExecutor.AbortPolicy());
    }

    public void start() {
        fetcher.start();
        loopThread = new Thread(this::loop, "task-scheduler");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    public void close() {
        stopped = true;
        fetcher.stop();
        pauseSignal.offer(Boolean.TRUE);
        if (loopThread != null) {
            loopThread.interrupt();
            try {
                loopThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        pool.shutdownNow();
    }

    /**
     * 等待空闲：任务队列清空、无在途任务，且上次拉取远端为空。
     * 远端任务可能持续产生，空闲只代表此刻没有待处理任务，之后可能有新任务到来
     *
     * @return true 表示已空闲
     */
    public boolean awaitIdle(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!isIdle()) {
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) {
                return false;
            }
            // 周期性重查空闲状态（远端拉取为空标志由抓取线程更新，无对应信号）
            Thread.sleep(Math.min(remain, 200));
        }
        return true;
    }

    public TaskResult getLastResult() {
        return lastResult;
    }

    public TaskResult getResult(String taskId) {
        return results.get(taskId);
    }

    public long getSubmittedCount() {
        return submittedCount.get();
    }

    public long getSucceededCount() {
        return succeededCount.get();
    }

    public long getResourceRejectedCount() {
        return resourceRejectedCount.get();
    }

    public long getPauseCount() {
        return pauseCount.get();
    }

    /**
     * 任务执行完成后回调（runner.run 返回后由线程池任务调用）：写入 lastResult、唤醒暂停或转入重试队列
     */
    void onTaskDone(Task task, TaskResult result) {
        results.put(task.getId(), result);
        lastResult = result;
        if (result.isResourceFull()) {
            resourceRejectedCount.incrementAndGet();
            taskQueue.putRetry(task);
            log("任务 " + task.getId() + " 资源池满快速失败(" + result.getElapsedMs()
                    + "ms, code=" + result.getCode() + ")，放回重试队列");
            // 资源未释放，不唤醒暂停等待
        } else {
            if (result.isSuccess()) {
                succeededCount.incrementAndGet();
            }
            log("任务 " + task.getId() + " " + (result.isSuccess() ? "执行成功" : "执行失败")
                    + ", code=" + result.getCode() + ", elapsed=" + result.getElapsedMs()
                    + "ms, inFlight=" + poolTaskCount());
            // 资源已释放，唤醒调度器的暂停等待
            pauseSignal.offer(Boolean.TRUE);
        }
    }

    private void loop() {
        try {
            while (!stopped) {
                //等待资源
                awaitResourcePool();
                //取任务
                Task task = nextTask();
                if (task == null) {
                    //没有取到任务，进入下一次循环
                    continue;
                }
                if (submit(task)) {
                    //提交成功，等待间隔时间
                    Thread.sleep(submitIntervalMs);
                } else {
                    // 提交失败，重试
                    taskQueue.putRetry(task);
                    log("提交失败，任务 " + task.getId() + " 放回重试队列，暂停等待");
                    await();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("调度线程结束");
    }

    /**
     * 取下一个任务：优先重试队列；两队列都为空则通知 TaskFetcher 抓取并阻塞等待
     */
    private Task nextTask() throws InterruptedException {
        Task task = taskQueue.poll();
        if (task == null) {
            fetcher.notifyFetch();
            task = taskQueue.poll(200);
        }
        return task;
    }

    /**
     * 提交前检查 lastResult：资源池满则暂停等待（超时 1s 或由非资源满结果唤醒）
     */
    private void awaitResourcePool() throws InterruptedException {
        // 根据上次结果判断是否资源满
        TaskResult last = lastResult;
        if (last == null || !last.isResourceFull()) {
            return;
        }
        pauseCount.incrementAndGet();
        log("资源池满(lastResult code=" + last.getCode() + ")，暂停等待");
        pauseSignal.poll(resourceFullPauseMs, TimeUnit.MILLISECONDS);
    }

    private boolean submit(Task task) {
        // 并发闸门：达到 maxConcurrent 不再提交，由调用方放回重试队列等待
        if (poolTaskCount() >= maxConcurrent) {
            return false;
        }
        try {
            pool.submit(() -> {
                TaskResult result;
                try {
                    result = runner.run(task);
                } catch (Throwable e) {
                    result = TaskResult.fail(task, ErrorCode.ERROR_INTERRUPTED, 0);
                }
                onTaskDone(task, result);
            });
            submittedCount.incrementAndGet();
            log("提交任务 " + task.getId() + ", 拉取队列=" + taskQueue.fetchSize()
                    + ", 重试队列=" + taskQueue.retrySize() + ", inFlight=" + poolTaskCount());
        } catch (RejectedExecutionException e) {
            return false;
        }
        return true;
    }

    private void await() throws InterruptedException {
        pauseSignal.poll(resourceFullPauseMs, TimeUnit.MILLISECONDS);
    }

    private boolean isIdle() {
        return fetcher.isLastFetchEmpty() && taskQueue.isEmpty() && poolTaskCount() == 0;
    }

    /**
     * 池内任务数：正在执行 + 在池队列排队中（getActiveCount 只统计前者）
     */
    private int poolTaskCount() {
        return pool.getActiveCount() + pool.getQueue().size();
    }

    private void log(String msg) {
        System.out.printf("[%6dms][%-14s] %s%n",
                System.currentTimeMillis() - startMs, Thread.currentThread().getName(), msg);
    }
}
