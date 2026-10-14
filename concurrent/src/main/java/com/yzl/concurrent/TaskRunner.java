package com.yzl.concurrent;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务执行器（mock 执行耗时）：
 * 按任务耗时执行并返回成功；执行中被中断返回 {@link ErrorCode#ERROR_INTERRUPTED}；
 * 以 resourceFullPercent 概率直接返回 {@link ErrorCode#ERROR_RESOURCE_FULL} 快速失败（不占用执行、50ms 内返回）。
 * 不含并发控制（并发由 {@link TaskScheduler} 的闸门把控），只负责执行并返回结果
 */
public class TaskRunner {

    private static final int DEFAULT_RESOURCE_FULL_PERCENT = 20;

    private final int resourceFullPercent;
    private final Random random;
    private final AtomicInteger current = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger succeeded = new AtomicInteger();

    public TaskRunner() {
        this(DEFAULT_RESOURCE_FULL_PERCENT);
    }

    public TaskRunner(int resourceFullPercent) {
        this(resourceFullPercent, new Random());
    }

    public TaskRunner(int resourceFullPercent, Random random) {
        this.resourceFullPercent = resourceFullPercent;
        this.random = random;
    }

    public TaskResult run(Task task) {
        long start = System.currentTimeMillis();
        if (random.nextInt(100) < resourceFullPercent) {
            return TaskResult.fail(task, ErrorCode.ERROR_RESOURCE_FULL,
                    System.currentTimeMillis() - start);
        }
        updateMax(current.incrementAndGet());
        try {
            Thread.sleep(task.getDurationMs());
            succeeded.incrementAndGet();
            return TaskResult.success(task, System.currentTimeMillis() - start);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.fail(task, ErrorCode.ERROR_INTERRUPTED,
                    System.currentTimeMillis() - start);
        } finally {
            current.decrementAndGet();
        }
    }

    public int getMaxConcurrent() {
        return maxConcurrent.get();
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
