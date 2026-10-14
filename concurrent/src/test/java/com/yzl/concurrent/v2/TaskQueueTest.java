package com.yzl.concurrent.v2;

import com.yzl.concurrent.Task;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskQueueTest {

    @Test(timeout = 30_000)
    public void pollPrefersRetryQueue() throws Exception {
        TaskQueue queue = new TaskQueue(5);
        Task fetchTask = new Task("fetch", 100);
        Task retryTask = new Task("retry", 100);
        queue.putFetch(fetchTask);
        queue.putRetry(retryTask);

        assertEquals("应优先取重试队列任务", retryTask, queue.poll());
        assertEquals(fetchTask, queue.poll());
        assertNull("两队列都为空应返回 null", queue.poll());
        assertEquals(0, queue.size());
    }

    @Test(timeout = 30_000)
    public void pollBlocksWhenBothEmptyAndWakesOnEitherQueue() throws Exception {
        TaskQueue queue = new TaskQueue(5);

        long start = System.currentTimeMillis();
        assertNull("两队列都为空应阻塞至超时", queue.poll(150));
        assertTrue("阻塞时长应接近超时值", System.currentTimeMillis() - start >= 100);

        // 拉取队列放入任务即唤醒
        AtomicReference<Task> fromFetch = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            try {
                fromFetch.set(queue.poll(3_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t1.start();
        Thread.sleep(100);
        Task a = new Task("a", 100);
        queue.putFetch(a);
        t1.join(3_000);
        assertEquals("拉取队列放入任务应唤醒阻塞的消费线程", a, fromFetch.get());

        // 重试队列放入任务即唤醒
        AtomicReference<Task> fromRetry = new AtomicReference<>();
        Thread t2 = new Thread(() -> {
            try {
                fromRetry.set(queue.poll(3_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t2.start();
        Thread.sleep(100);
        Task b = new Task("b", 100);
        queue.putRetry(b);
        t2.join(3_000);
        assertEquals("重试队列放入任务应唤醒阻塞的消费线程", b, fromRetry.get());
    }

    @Test(timeout = 30_000)
    public void putNeverBlocksAndMayExceedCapacity() throws Exception {
        TaskQueue queue = new TaskQueue(2);
        assertEquals(2, queue.fetchCapacity());

        // put 纯 offer 不阻塞：连放 5 个拉取任务，长度超过容量
        for (int i = 1; i <= 5; i++) {
            queue.putFetch(new Task("f" + i, 100));
        }
        assertEquals("putFetch 不阻塞且允许超过容量", 5, queue.fetchSize());

        Task r1 = new Task("r1", 100);
        Task r2 = new Task("r2", 100);
        queue.putRetry(r1);
        queue.putRetry(r2);
        assertEquals("两队列长度独立累加", 2, queue.retrySize());
        assertEquals(7, queue.size());
        assertEquals("峰值应记录两队列之和", 7, queue.getMaxObservedSize());

        // 取出时优先重试队列
        assertEquals(r1, queue.poll());
        assertEquals(r2, queue.poll());

        int drained = 0;
        while (queue.poll() != null) {
            drained++;
        }
        assertEquals(5, drained);
        assertTrue(queue.isEmpty());
    }
}
