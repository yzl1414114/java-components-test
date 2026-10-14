package com.yzl.concurrent;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 端到端：mock HTTP 任务服务 + 资源受限执行器，验证
 * 拉取 -> 并发执行 -> 资源不足快速失败 -> 缓存 -> 完成后重试 -> 全部完成
 */
public class TaskSchedulerIntegrationTest {

    @Test(timeout = 60_000)
    public void endToEndFetchExecuteCacheAndRetry() throws Exception {
        int taskCount = 12;
        MockTaskServer server = new MockTaskServer(taskCount, 300, 800, new Random(7));
        server.start();
        TaskScheduler scheduler = null;
        try {
            SimulatedTaskRunner runner = new SimulatedTaskRunner(2, 15);
            scheduler = new TaskScheduler(
                    new HttpTaskClient("http://127.0.0.1:" + server.getPort()),
                    runner,
                    8,      // 工作线程数（> 槽位数）
                    3,      // 每次拉取 3 个，槽位只有 2，必然触发资源不足
                    1000,   // 试探间隔（测试用较短的 1s）
                    300);   // 轮询间隔
            scheduler.start();

            assertTrue("调度未在时限内空闲", scheduler.awaitIdle(45_000));

            assertEquals("远端任务应全部取完", 0, server.remaining());
            assertEquals("所有任务应恰好成功一次", taskCount, scheduler.getSucceededCount());
            assertEquals(taskCount, scheduler.getFetchedCount());
            assertEquals("快速失败次数应与 runner 统计一致",
                    runner.getRejectedCount(), scheduler.getResourceRejectedCount());
            assertTrue("应发生过资源不足快速失败", scheduler.getResourceRejectedCount() > 0);
            assertTrue("应发生过任务缓存重试", scheduler.getCachedCount() > 0);
            assertTrue("峰值并发不应超过资源槽位, actual=" + runner.getMaxConcurrent(),
                    runner.getMaxConcurrent() <= 2);

            List<TaskResult> successResults = allSuccess(scheduler, taskCount);
            assertEquals(taskCount, successResults.size());
        } finally {
            if (scheduler != null) {
                scheduler.close();
            }
            server.stop();
        }
    }

    private List<TaskResult> allSuccess(TaskScheduler scheduler, int taskCount) {
        List<TaskResult> list = new ArrayList<>();
        for (int i = 1; i <= taskCount; i++) {
            TaskResult r = scheduler.getResult(String.format("task-%04d", i));
            assertNotNull("任务 task-" + String.format("%04d", i) + " 应存在成功结果", r);
            assertTrue("任务应成功: " + r, r.isSuccess());
            list.add(r);
        }
        return list;
    }
}
