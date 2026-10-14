package com.yzl.consume;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * mock 远端任务服务：GET /tasks?limit=n 返回 JSON 任务数组，任务按预生成队列下发
 */
public class MockTaskServer {

    private final HttpServer server;
    private final ConcurrentLinkedQueue<Task> backlog = new ConcurrentLinkedQueue<>();
    private final AtomicLong dispatched = new AtomicLong();
    private final AtomicLong requestCount = new AtomicLong();

    public MockTaskServer(int taskCount, long minDurationMs, long maxDurationMs, Random rnd) throws IOException {
        for (int i = 0; i < taskCount; i++) {
            long duration = minDurationMs + (long) (rnd.nextDouble() * (maxDurationMs - minDurationMs + 1));
            backlog.add(new Task(String.format("task-%04d", i + 1), duration));
        }
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "mock-task-server-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/tasks", this::handleTasks);
        server.setExecutor(Executors.newFixedThreadPool(2, tf));
    }

    public void start() {
        server.start();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public int remaining() {
        return backlog.size();
    }

    public long getDispatchedCount() {
        return dispatched.get();
    }

    public long getRequestCount() {
        return requestCount.get();
    }

    public void stop() {
        server.stop(0);
    }

    private void handleTasks(HttpExchange exchange) throws IOException {
        requestCount.incrementAndGet();
        try {
            int limit = parseLimit(exchange.getRequestURI().getRawQuery());
            StringBuilder json = new StringBuilder("[");
            boolean first = true;
            for (int i = 0; i < limit; i++) {
                Task t = backlog.poll();
                if (t == null) {
                    break;
                }
                dispatched.incrementAndGet();
                if (!first) {
                    json.append(',');
                }
                first = false;
                json.append("{\"id\":\"").append(t.getId())
                        .append("\",\"durationMs\":").append(t.getDurationMs()).append('}');
            }
            json.append(']');
            byte[] body = json.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            OutputStream os = exchange.getResponseBody();
            try {
                os.write(body);
            } finally {
                os.close();
            }
        } finally {
            exchange.close();
        }
    }

    private static int parseLimit(String query) {
        if (query == null) {
            return 3;
        }
        for (String pair : query.split("&")) {
            if (pair.startsWith("limit=")) {
                try {
                    return Math.max(1, Integer.parseInt(pair.substring("limit=".length())));
                } catch (NumberFormatException ignore) {
                    // fall through to default
                }
            }
        }
        return 3;
    }
}
