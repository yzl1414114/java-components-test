package com.yzl.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mock 任务执行：获取资源槽位后按任务耗时执行；
 * 资源不足时任务启动执行后 failDelayMs 内返回 {@link ErrorCode#ERROR_RESOURCE_FULL}（须 &lt; 50ms）
 */
public class SimulatedTaskRunner implements TaskRunner {

    private final Semaphore slots;
    private final long failDelayMs;
    private final AtomicInteger current = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();
    private final AtomicInteger succeeded = new AtomicInteger();

    public SimulatedTaskRunner(int maxSlots, long failDelayMs) {
        this.slots = new Semaphore(maxSlots);
        this.failDelayMs = failDelayMs;
    }

    @Override
    public TaskResult run(Task task) {
        long start = System.currentTimeMillis();
        if (!slots.tryAcquire()) {
            try {
                Thread.sleep(failDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            rejected.incrementAndGet();
            return TaskResult.fail(task, ErrorCode.ERROR_RESOURCE_FULL, System.currentTimeMillis() - start);
        }
        int now = current.incrementAndGet();
        updateMax(now);
        try {
            Thread.sleep(task.getDurationMs());
            succeeded.incrementAndGet();
            return TaskResult.success(task, System.currentTimeMillis() - start);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.fail(task, ErrorCode.ERROR_INTERRUPTED, System.currentTimeMillis() - start);
        } finally {
            current.decrementAndGet();
            slots.release();
        }
    }

    public int getMaxConcurrent() {
        return maxConcurrent.get();
    }

    public int getRejectedCount() {
        return rejected.get();
    }

    public int getSucceededCount() {
        return succeeded.get();
    }

    private void updateMax(int now) {
        int prev;
        do {
            prev = maxConcurrent.get();
            if (now <= prev) {
                return;
            }
        } while (!maxConcurrent.compareAndSet(prev, now));
    }
}