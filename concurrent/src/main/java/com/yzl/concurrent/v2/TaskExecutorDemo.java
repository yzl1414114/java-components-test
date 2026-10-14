package com.yzl.concurrent.v2;

import com.yzl.concurrent.HttpTaskClient;
import com.yzl.concurrent.MockTaskServer;

import java.util.Random;

/**
 * 手动演示（v2 思路）：本地队列(10) 预取 + 提交节流(10ms) + 并发闸门(4) + 20% 资源满快速失败重试
 */
public class TaskExecutorDemo {

    public static void main(String[] args) throws Exception {
        int taskCount = 20;
        MockTaskServer server = new MockTaskServer(taskCount, 1000, 5000, new Random());
        server.start();
        System.out.println("mock 任务服务已启动: http://127.0.0.1:" + server.getPort()
                + "/tasks, 任务数=" + taskCount + ", 耗时 1~5s");

        TaskFetcher fetcher = new TaskFetcher(
                new HttpTaskClient("http://127.0.0.1:" + server.getPort()),
                4);                                     // 每次最多抓 4 个，本地队列上限 10
        TaskRunner runner = new TaskRunner();
        TaskScheduler scheduler = new TaskScheduler(
                fetcher, runner,
                4,      // 并发闸门（模拟资源池 4 槽，线程池自动放大 5）
                1000,   // 闸门拦截暂停时长
                10);    // 每次提交后节流 10ms

        scheduler.start();
        boolean idle = scheduler.awaitIdle(300_000);

        System.out.println();
        System.out.println("==== 执行结束 (idle=" + idle + ") ====");
        System.out.println("抓取任务数     : " + fetcher.getFetchedCount());
        System.out.println("提交次数       : " + scheduler.getSubmittedCount());
        System.out.println("执行成功数     : " + scheduler.getSucceededCount());
        System.out.println("资源满拒绝数   : " + scheduler.getResourceRejectedCount());
        System.out.println("峰值并发       : " + runner.getMaxConcurrent());
        System.out.println("本地队列峰值   : " + fetcher.getTaskQueue().getMaxObservedSize());
        System.out.println("最近一次结果   : " + scheduler.getLastResult());
        System.out.println("服务端剩余任务 : " + server.remaining());

        scheduler.close();
        server.stop();
    }
}
