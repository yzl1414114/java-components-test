package com.yzl.concurrent;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimulatedTaskRunnerTest {

    @Test
    public void successAfterSimulatedWork() {
        SimulatedTaskRunner runner = new SimulatedTaskRunner(2, 15);
        long start = System.currentTimeMillis();
        TaskResult r = runner.run(new Task("t1", 200));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(r.isSuccess());
        assertEquals(ErrorCode.SUCCESS, r.getCode());
        assertTrue("应实际执行约 200ms, actual=" + elapsed, elapsed >= 200);
    }

    @Test
    public void rejectWithErrorCodeWithin50msWhenNoResource() throws Exception {
        SimulatedTaskRunner runner = new SimulatedTaskRunner(1, 15);
        Thread holder = new Thread(() -> runner.run(new Task("holder", 500)));
        holder.start();
        Thread.sleep(100);

        long start = System.currentTimeMillis();
        TaskResult r = runner.run(new Task("fast-fail", 100));
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(ErrorCode.ERROR_RESOURCE_FULL, r.getCode());
        assertTrue("资源不足时应在 50ms 内返回错误码, actual=" + elapsed + "ms", elapsed < 50);

        holder.join(2000);
    }

    @Test
    public void concurrentRunsNeverExceedSlots() throws Exception {
        int slots = 3;
        SimulatedTaskRunner runner = new SimulatedTaskRunner(slots, 15);
        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            List<Future<TaskResult>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> runner.run(new Task("t" + idx, 200))));
            }
            int success = 0;
            int rejected = 0;
            for (Future<TaskResult> f : futures) {
                TaskResult r = f.get();
                if (r.isSuccess()) {
                    success++;
                } else if (r.isResourceFull()) {
                    rejected++;
                }
            }
            assertEquals("槽位不足的任务应快速失败", 7, rejected);
            assertEquals("同一时刻最多执行 slots 个任务", 3, success);
            assertTrue("峰值并发不应超过槽位数, actual=" + runner.getMaxConcurrent(),
                    runner.getMaxConcurrent() <= slots);
        } finally {
            pool.shutdownNow();
        }
    }
}
