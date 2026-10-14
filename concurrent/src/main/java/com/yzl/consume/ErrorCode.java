package com.yzl.consume;

public final class ErrorCode {

    public static final int SUCCESS = 0;

    /** 资源不足时任务启动执行后快速失败返回的指定错误码（50ms 内返回） */
    public static final int ERROR_RESOURCE_FULL = 42901;

    public static final int ERROR_INTERRUPTED = 499;

    private ErrorCode() {
    }
}
