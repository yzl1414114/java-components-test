package com.yzl.concurrent;

/**
 * 任务执行器：资源不足时需在 50ms 内返回 {@link ErrorCode#ERROR_RESOURCE_FULL}
 */
public interface TaskRunner {

    TaskResult run(Task task);
}
