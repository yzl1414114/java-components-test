package com.yzl.consume;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 端到端（v2 思路）：mock 服务端 -> TaskFetcher 本地队列(10) -> 调度节流 + 并发闸门 ->
 * runner 执行（20% 资源满快速失败 -> 重试）回写 lastResult
 */
public class TaskSchedulerIntegrationTest {

    @Test(timeout = 90_000)
    public void fetchExecuteThrottleGateAndResourceFullRetry() throws Exception {
        int taskCount = 12;
        MockTaskServer server = new MockTaskServer(taskCount, 300, 800, new Random(7));
        server.start();
        TaskScheduler scheduler = null;
        try {
            TaskFetcher fetcher = new TaskFetcher(
                    new HttpTaskClient("http://127.0.0.1:" + server.getPort()), 4);
            // seed 7 的随机序列在 12 个任务收敛前确定产生 4 次资源满快速失败
            TaskRunner runner = new TaskRunner(20, new Random(7));
            scheduler = new TaskScheduler(fetcher, runner, 2, 1000, 10);   // 并发闸门 2
            scheduler.start();

            assertTrue("调度未在时限内空闲", scheduler.awaitIdle(60_000));

            assertEquals("远端任务应全部取完", 0, server.remaining());
            assertEquals("任务应全部抓取", taskCount, fetcher.getFetchedCount());
            assertEquals("所有任务应恰好成功一次", taskCount, scheduler.getSucceededCount());
            assertTrue("应发生过资源池满快速失败", scheduler.getResourceRejectedCount() > 0);
            assertEquals("提交次数 = 任务数 + 资源满失败重试次数",
                    taskCount + scheduler.getResourceRejectedCount(), scheduler.getSubmittedCount());
            assertTrue("峰值并发不应超过闸门, actual=" + runner.getMaxConcurrent(),
                    runner.getMaxConcurrent() <= 2);
            assertTrue("两队列总长不应超过拉取容量 + 一批次, actual=" + fetcher.getTaskQueue().getMaxObservedSize(),
                    fetcher.getTaskQueue().getMaxObservedSize()
                    <= TaskQueue.DEFAULT_FETCH_CAPACITY + 4);
            assertNotNull(scheduler.getLastResult());
            assertTrue("最近一次结果应为成功", scheduler.getLastResult().isSuccess());

            for (int i = 1; i <= taskCount; i++) {
                String id = String.format("task-%04d", i);
                TaskResult r = scheduler.getResult(id);
                assertNotNull("任务 " + id + " 应存在结果", r);
                assertTrue("任务 " + id + " 应最终成功: " + r, r.isSuccess());
            }
        } finally {
            if (scheduler != null) {
                scheduler.close();
            }
            server.stop();
        }
    }

    @Test(timeout = 30_000)
    public void requeueWhenGateFullAndPauseUntilWorkerFrees() throws Exception {
        MockTaskServer server = new MockTaskServer(2, 300, 500, new Random(11));
        server.start();
        TaskScheduler scheduler = null;
        try {
            TaskFetcher fetcher = new TaskFetcher(
                    new HttpTaskClient("http://127.0.0.1:" + server.getPort()), 2);
            TaskRunner runner = new TaskRunner(20, new Random(11));
            // 并发闸门只有 1，第二个任务提交时被闸门拦截，走"放回重试队列 + 暂停等待"分支
            scheduler = new TaskScheduler(fetcher, runner, 1, 1000, 10);
            scheduler.start();

            assertTrue("调度未在时限内空闲", scheduler.awaitIdle(20_000));
            assertEquals("被闸门拦截放回重试后应最终全部成功", 2, scheduler.getSucceededCount());
            assertEquals("提交次数 = 任务数 + 资源满失败重试次数",
                    2 + scheduler.getResourceRejectedCount(), scheduler.getSubmittedCount());
            assertEquals("远端任务应全部取完", 0, server.remaining());
        } finally {
            if (scheduler != null) {
                scheduler.close();
            }
            server.stop();
        }
    }
}
