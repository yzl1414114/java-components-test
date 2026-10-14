package com.yzl.consume;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务抓取器：从远端 HTTP 服务获取任务，放入本地 {@link TaskQueue 拉取队列}。
 * 队列为空时由 {@link TaskScheduler} 通过 {@link #notifyFetch()} 触发补货，每次抓取一批；
 * 两队列长度之和大于拉取队列容量（默认 {@link TaskQueue#DEFAULT_FETCH_CAPACITY}）即停止抓取。
 * 远端任务可能持续产生，不判定"耗尽"：只记录上次拉取是否为空，为空时抑制唤醒，
 * 由抓取线程每 10s 超时兜底重试，避免服务端请求压力过大
 */
public class TaskFetcher {

    private final TaskClient client;
    private final int fetchBatch;
    /** 本地任务队列（拉取队列 / 重试队列）：与 {@link TaskScheduler} 共用同一实例 */
    private final TaskQueue taskQueue = new TaskQueue();
    /** 抓取触发信号：容量 1，令牌存在即代表有待处理的抓取请求（重复 offer 自动合并） */
    private final BlockingQueue<Boolean> trigger = new ArrayBlockingQueue<>(1);
    /** 上次拉取远端是否为空；为空时抑制 notifyFetch 唤醒，降低服务端请求压力 */
    private volatile boolean lastFetchEmpty;
    private final AtomicLong fetchedCount = new AtomicLong();
    private final long startMs = System.currentTimeMillis();

    private volatile boolean stopped;
    private Thread thread;

    public TaskFetcher(TaskClient client, int fetchBatch) {
        this.client = client;
        this.fetchBatch = fetchBatch;
    }

    /** 启动抓取线程并预取一轮 */
    public void start() {
        if (thread != null) {
            return;
        }
        thread = new Thread(this::loop, "task-fetcher");
        thread.setDaemon(true);
        thread.start();
        notifyFetch();
    }

    public void stop() {
        stopped = true;
        trigger.offer(Boolean.TRUE);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 通知抓取线程执行抓取（令牌已存在时 offer 失败，请求合并即可）。
     * 上次拉取远端为空时抑制唤醒，等抓取线程 10s 超时兜底重试，避免服务端请求压力过大
     */
    public void notifyFetch() {
        if (lastFetchEmpty) {
            return;
        }
        trigger.offer(Boolean.TRUE);
    }

    /** 本地任务队列，供 {@link TaskScheduler} 取任务与放回重试任务 */
    public TaskQueue getTaskQueue() {
        return taskQueue;
    }

    /** 上次拉取远端是否为空 */
    public boolean isLastFetchEmpty() {
        return lastFetchEmpty;
    }

    public long getFetchedCount() {
        return fetchedCount.get();
    }

    private void loop() {
        while (!stopped) {
            try {
                // 阻塞等待抓取请求，最多 10 秒；超时兜底补货一次（上次远端为空时的唯一重试途径）
                trigger.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!stopped) {
                fillQueue();
            }
        }
    }

    private void fillQueue() {
        try {
            while (!stopped) {
                // 满判定：两队列长度之和大于拉取队列容量即暂停抓取
                if (taskQueue.size() > taskQueue.fetchCapacity()) {
                    log("任务队列已满(两队列和=" + taskQueue.size() + " > " + taskQueue.fetchCapacity() + ")，停止抓取");
                    return;
                }
                List<Task> batch = client.fetch(fetchBatch);
                if (batch.isEmpty()) {
                    lastFetchEmpty = true;
                    log("远端本次拉取为空，抑制主动抓取，等 10s 超时兜底重试");
                    return;
                }
                lastFetchEmpty = false;
                for (Task task : batch) {
                    taskQueue.putFetch(task);
                    fetchedCount.incrementAndGet();
                }
                log("抓取 " + batch.size() + " 个任务，两队列和 " + taskQueue.size() + "/" + taskQueue.fetchCapacity());
            }
        } catch (IOException e) {
            log("抓取任务失败: " + e.getMessage());
        }
    }

    private void log(String msg) {
        System.out.printf("[%6dms][%-14s] %s%n",
                System.currentTimeMillis() - startMs, Thread.currentThread().getName(), msg);
    }
}
