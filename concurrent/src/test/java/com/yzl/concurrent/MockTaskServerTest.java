package com.yzl.concurrent;

import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MockTaskServerTest {

    @Test
    public void fetchTasksOverHttp() throws Exception {
        MockTaskServer server = new MockTaskServer(5, 100, 200, new Random(42));
        server.start();
        try {
            HttpTaskClient client = new HttpTaskClient("http://127.0.0.1:" + server.getPort());

            List<Task> batch1 = client.fetch(3);
            assertEquals(3, batch1.size());

            List<Task> batch2 = client.fetch(3);
            assertEquals(2, batch2.size());

            assertTrue("任务取完后应返回空列表", client.fetch(3).isEmpty());

            for (Task t : batch1) {
                assertTrue(t.getId().startsWith("task-"));
                assertTrue("任务耗时应在生成范围内: " + t,
                        t.getDurationMs() >= 100 && t.getDurationMs() <= 200);
            }
            assertEquals(5, server.getDispatchedCount());
            assertEquals(0, server.remaining());
        } finally {
            server.stop();
        }
    }
}
