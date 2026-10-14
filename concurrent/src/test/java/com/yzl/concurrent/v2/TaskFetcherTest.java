package com.yzl.concurrent.v2;

import com.yzl.concurrent.HttpTaskClient;
import com.yzl.concurrent.MockTaskServer;
import org.junit.Test;

import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TaskFetcherTest {

    @Test(timeout = 30_000)
    public void fetchStopsWhenTotalExceedsFetchCapacity() throws Exception {
        MockTaskServer server = new MockTaskServer(15, 100, 200, new Random(1));
        server.start();
        TaskFetcher fetcher = new TaskFetcher(
                new HttpTaskClient("http://127.0.0.1:" + server.getPort()), 4);
        try {
            fetcher.start();

            // 每次抓 4 个，两队列之和超过 fetchCapacity(10) 即停止：4 + 4 + 4 = 12
            assertTrue("两队列之和应在时限内超过上限",
                    waitUntil(() -> fetcher.getTaskQueue().getMaxObservedSize() == 12, 5_000));
            assertEquals("满判定后应恰好超出一批", 12, fetcher.getTaskQueue().size());
            assertEquals("满判定后不应继续抓取", 12, server.getDispatchedCount());

            // 消费 3 个后再次触发抓取：9 + 3 = 12，仍超上限即停
            for (int i = 0; i < 3; i++) {
                assertNotNull(fetcher.getTaskQueue().poll());
            }
            assertEquals(9, fetcher.getTaskQueue().size());
            fetcher.notifyFetch();
            assertTrue("应再抓一批补货(远端剩 3 个)",
                    waitUntil(() -> fetcher.getTaskQueue().size() == 12, 5_000));
            assertEquals(15, server.getDispatchedCount());
            assertFalse("远端仍有任务时上次拉取不应为空", fetcher.isLastFetchEmpty());
        } finally {
            fetcher.stop();
            server.stop();
        }
    }

    @Test(timeout = 30_000)
    public void lastFetchEmptyAfterRemoteDrained() throws Exception {
        MockTaskServer server = new MockTaskServer(5, 100, 200, new Random(2));
        server.start();
        TaskFetcher fetcher = new TaskFetcher(
                new HttpTaskClient("http://127.0.0.1:" + server.getPort()), 3);
        try {
            fetcher.start();
            // 抓取线程一直补货直到队列满或远端返回空：5 个任务取完后即命中一次空拉取
            assertTrue("应抓取到全部 5 个任务", waitUntil(() -> fetcher.getFetchedCount() == 5, 5_000));
            assertTrue("远端取空后应标记上次拉取为空", waitUntil(fetcher::isLastFetchEmpty, 5_000));

            int drained = 0;
            while (fetcher.getTaskQueue().poll() != null) {
                drained++;
            }
            assertEquals(5, drained);
            assertEquals(0, server.remaining());
        } finally {
            fetcher.stop();
            server.stop();
        }
    }

    @Test(timeout = 30_000)
    public void suppressFetchWhenLastFetchEmpty() throws Exception {
        MockTaskServer server = new MockTaskServer(3, 100, 200, new Random(3));
        server.start();
        TaskFetcher fetcher = new TaskFetcher(
                new HttpTaskClient("http://127.0.0.1:" + server.getPort()), 2);
        try {
            fetcher.start();
            assertTrue("远端取空后应标记上次拉取为空", waitUntil(fetcher::isLastFetchEmpty, 5_000));
            while (fetcher.getTaskQueue().poll() != null) {
                // 取空本地队列
            }
            assertEquals(0, fetcher.getTaskQueue().size());

            long requestsBefore = server.getRequestCount();
            for (int i = 0; i < 5; i++) {
                fetcher.notifyFetch();   // 上次拉取为空 → 抑制唤醒，不应产生新请求
            }
            Thread.sleep(500);            // 10s 超时兜底尚未到
            assertEquals("上次拉取为空时应抑制唤醒，避免服务端压力过大",
                    requestsBefore, server.getRequestCount());
        } finally {
            fetcher.stop();
            server.stop();
        }
    }

    private static boolean waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }
}
