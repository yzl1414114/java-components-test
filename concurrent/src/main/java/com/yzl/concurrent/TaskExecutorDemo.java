package com.yzl.concurrent;

import java.util.Random;

/**
 * 手动演示：mock 服务端(20 个任务, 1~5s) + 4 资源槽位执行器，观察缓存重试/试探行为
 */
public class TaskExecutorDemo {

    public static void main(String[] args) throws Exception {
        int taskCount = 20;
        MockTaskServer server = new MockTaskServer(taskCount, 1000, 5000, new Random());
        server.start();
        System.out.println("mock 任务服务已启动: http://127.0.0.1:" + server.getPort()
                + "/tasks, 任务数=" + taskCount + ", 耗时 1~5s");

        SimulatedTaskRunner runner = new SimulatedTaskRunner(4, 15);
        TaskScheduler scheduler = new TaskScheduler(
                new HttpTaskClient("http://127.0.0.1:" + server.getPort()),
                runner,
                10,     // 工作线程数（> 资源槽位，保证快速失败可被观察）
                3,      // 每次拉取任务数
                5000,   // 资源不足时试探间隔
                500);   // 正常轮询间隔
        scheduler.start();

        boolean idle = scheduler.awaitIdle(300_000);
        System.out.println();
        System.out.println("==== 执行结束 (idle=" + idle + ") ====");
        System.out.println("拉取任务数     : " + scheduler.getFetchedCount());
        System.out.println("执行成功数     : " + scheduler.getSucceededCount());
        System.out.println("资源不足快速失败: " + scheduler.getResourceRejectedCount() + " 次");
        System.out.println("缓存重试次数   : " + scheduler.getCachedCount());
        System.out.println("试探执行次数   : " + scheduler.getProbeCount());
        System.out.println("峰值并发       : " + runner.getMaxConcurrent());
        System.out.println("服务端剩余任务 : " + server.remaining());

        scheduler.close();
        server.stop();
    }
}
