package com.yzl.consume;

/**
 * 远端任务
 */
public class Task {

    private final String id;
    /** 任务大小：模拟执行耗时，任务一般在 1~5 秒内完成 */
    private final long durationMs;

    public Task(String id, long durationMs) {
        this.id = id;
        this.durationMs = durationMs;
    }

    public String getId() {
        return id;
    }

    public long getDurationMs() {
        return durationMs;
    }

    @Override
    public String toString() {
        return "Task{id='" + id + "', durationMs=" + durationMs + "}";
    }
}
