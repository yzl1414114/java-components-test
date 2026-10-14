package com.yzl.concurrent.v2;

import com.yzl.concurrent.ErrorCode;
import com.yzl.concurrent.Task;
import com.yzl.concurrent.TaskResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TaskRunnerTest {

    @Test
    public void runReturnsSuccessWithTaskDuration() {
        TaskRunner runner = new TaskRunner(0);

        TaskResult r = runner.run(new Task("single", 200));

        assertTrue("正常执行应成功: " + r, r.isSuccess());
        assertEquals(ErrorCode.SUCCESS, r.getCode());
        assertTrue("执行耗时不应小于任务时长, actual=" + r.getElapsedMs() + "ms",
                r.getElapsedMs() >= 200);
        assertEquals(1, runner.getSucceededCount());
    }

    @Test
    public void resourceFullReturnsErrorCodeWithin50ms() {
        TaskRunner runner = new TaskRunner(100);

        long start = System.currentTimeMillis();
        TaskResult r = runner.run(new Task("fast-fail", 5000));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("资源满结果应带 ERROR_RESOURCE_FULL: " + r, r.isResourceFull());
        assertEquals(ErrorCode.ERROR_RESOURCE_FULL, r.getCode());
        assertTrue("资源满应在 50ms 内返回, actual=" + elapsed + "ms", elapsed < 50);
        assertEquals("快速失败不应计入成功数", 0, runner.getSucceededCount());
    }

    @Test
    public void interruptedRunReturnsInterruptedCode() throws Exception {
        TaskRunner runner = new TaskRunner(0);
        AtomicReference<TaskResult> holder = new AtomicReference<>();

        Thread worker = new Thread(() -> holder.set(runner.run(new Task("long", 60_000))));
        worker.start();
        Thread.sleep(100);
        worker.interrupt();
        worker.join(2000);

        assertNotNull("中断后应返回结果", holder.get());
        assertEquals(ErrorCode.ERROR_INTERRUPTED, holder.get().getCode());
    }

    @Test
    public void maxConcurrentTracksActualConcurrency() throws Exception {
        TaskRunner runner = new TaskRunner(0);
        int threads = 3;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<TaskResult>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return runner.run(new Task("t" + idx, 300));
                }));
            }
            start.countDown();
            for (Future<TaskResult> f : futures) {
                assertTrue("并发执行的任务应全部成功", f.get().isSuccess());
            }
            assertEquals("无闸门时峰值并发应等于同时执行的任务数",
                    threads, runner.getMaxConcurrent());
        } finally {
            pool.shutdownNow();
        }
    }
}
