package com.yzl.consume;

import java.io.IOException;
import java.util.List;

/**
 * 远端任务获取客户端
 */
public interface TaskClient {

    /**
     * 从远端服务拉取最多 max 个任务
     */
    List<Task> fetch(int max) throws IOException;
}
