package com.yzl.consume;

public class TaskResult {

    private final Task task;
    private final int code;
    /** 本次执行（含快速失败）耗时 */
    private final long elapsedMs;

    private TaskResult(Task task, int code, long elapsedMs) {
        this.task = task;
        this.code = code;
        this.elapsedMs = elapsedMs;
    }

    public static TaskResult success(Task task, long elapsedMs) {
        return new TaskResult(task, ErrorCode.SUCCESS, elapsedMs);
    }

    public static TaskResult fail(Task task, int code, long elapsedMs) {
        return new TaskResult(task, code, elapsedMs);
    }

    public Task getTask() {
        return task;
    }

    public int getCode() {
        return code;
    }

    public long getElapsedMs() {
        return elapsedMs;
    }

    public boolean isSuccess() {
        return code == ErrorCode.SUCCESS;
    }

    public boolean isResourceFull() {
        return code == ErrorCode.ERROR_RESOURCE_FULL;
    }

    @Override
    public String toString() {
        return "TaskResult{task=" + task.getId() + ", code=" + code + ", elapsedMs=" + elapsedMs + "}";
    }
}
