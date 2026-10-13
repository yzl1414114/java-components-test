package com.yzl.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lettuce 非连接池模式多线程模型 Demo
 * <p>
 * 不连接真实 Redis，用「原生 Socket 服务端模拟 Redis」+「Netty 客户端复刻 Lettuce 内部结构」，
 * 演示一条 TCP 连接如何被任意多个业务线程安全共享：
 * <pre>
 * 业务线程A ──┐
 * 业务线程B ──┼─→ ①无锁队列(ConcurrentLinkedQueue) ─→ ②EventLoop 单线程批量写出 ─→ socket
 * 业务线程C ──┘                                                            │
 *                                                                          ▼
 * 业务线程被 future.get() 唤醒 ←─ ④CompletableFuture.complete() ←─ ③IO线程按FIFO
 *                                                                     匹配队头命令
 * </pre>
 * 与 Lettuce 源码的对应关系：
 * <ul>
 *   <li>MockRedisServer            ~ Redis 服务端（单连接内命令严格按 FIFO 串行处理、按序回复）</li>
 *   <li>Command                    ~ io.lettuce.core.protocol.Command（内嵌 RedisFuture/CompletableFuture）</li>
 *   <li>LettuceLikeConnection      ~ StatefulRedisConnectionImpl + DefaultEndpoint（写队列与 flush）</li>
 *   <li>pendingWrites              ~ DefaultEndpoint 内的 ConcurrentLinkedQueue（无锁命令队列）</li>
 *   <li>flushPendingWrites()       ~ EventLoop 线程批量 write + 一次 flush（天然 pipelining）</li>
 *   <li>ClientHandler#channelRead0 ~ CommandHandler.channelRead()（FIFO 取队头命令，complete 唤醒）</li>
 *   <li>dispatchSync()             ~ sync() API（阻塞调用线程，不阻塞连接）</li>
 * </ul>
 *
 * @author yutu
 * @date 2026/9/16
 */
public class TestLettuceThreadingModel {

    private static final int COMMAND_COST_MS = 100;

    // ==================== 1. Socket 服务端：模拟 Redis ====================

    /**
     * 每条连接一个处理线程、连接内严格串行：读一行(UTF-8) → sleep 100ms → 加随机数回写。
     * 与 Redis 单线程语义一致：同一连接内的响应顺序严格等于命令到达顺序（这是客户端 FIFO 匹配的前提）。
     */
    static class MockRedisServer implements Closeable {
        private final ServerSocket serverSocket;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final AtomicInteger processed = new AtomicInteger();
        private volatile boolean running = true;

        private MockRedisServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        static MockRedisServer start() throws IOException {
            ServerSocket serverSocket = new ServerSocket(0);
            MockRedisServer server = new MockRedisServer(serverSocket);
            server.executor.submit(server::acceptLoop);
            System.out.println("[MockRedisServer] 启动于 127.0.0.1:" + serverSocket.getLocalPort());
            return server;
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    executor.submit(() -> serve(socket));
                } catch (IOException e) {
                    // 服务端关闭，退出
                }
            }
        }

        private void serve(Socket socket) {
            try (Socket s = socket;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Thread.sleep(COMMAND_COST_MS); // 模拟 Redis 执行命令的耗时
                    String resp = line + ":" + ThreadLocalRandom.current().nextInt(1000);
                    writer.write(resp);
                    writer.newLine();
                    writer.flush();
                    int n = processed.incrementAndGet();
                    if (n <= 5) {
                        System.out.println("[MockRedisServer] 第" + n + "条命令处理完成: " + resp
                                + " (处理线程: " + Thread.currentThread().getName() + ")");
                    }
                }
            } catch (Exception e) {
                // 客户端断开，忽略
            }
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        int getProcessed() {
            return processed.get();
        }

        @Override
        public void close() throws IOException {
            running = false;
            serverSocket.close();
            executor.shutdownNow();
        }
    }

    // ==================== 2. 命令对象：对应 Lettuce 的 Command + RedisFuture ====================

    static class Command {
        final String content;
        /** 对应 RedisFuture：响应到达时由 IO 线程 complete，阻塞在 get() 的业务线程被唤醒 */
        final CompletableFuture<String> future = new CompletableFuture<>();

        Command(String content) {
            this.content = content;
        }
    }

    // ==================== 3. 客户端连接：对应 StatefulRedisConnection + DefaultEndpoint ====================

    /**
     * 一条 TCP 连接 + 一个 IO 线程(EventLoop) + 无锁命令队列 = 天然线程安全，无需连接池。
     * 任意业务线程可并发调用 dispatchAsync/dispatchSync。
     */
    static class LettuceLikeConnection implements Closeable {
        /** 发送侧无锁队列：任意业务线程并发入队，对应 DefaultEndpoint 内的 ConcurrentLinkedQueue */
        private final ConcurrentLinkedQueue<Command> pendingWrites = new ConcurrentLinkedQueue<>();
        /** 已写出、待响应队列：仅 EventLoop(IO)线程访问（与 Lettuce 一致，无需加锁），对应 CommandHandler 的 FIFO 匹配 */
        private final ArrayDeque<Command> inFlight = new ArrayDeque<>();
        /** Lettuce 中多条连接复用一个 EventLoopGroup(默认约等于 CPU 核数)，这里单连接绑定一个 IO 线程即可 */
        private final EventLoopGroup group = new NioEventLoopGroup(1, r -> {
            Thread t = new Thread(r, "redis-io-thread");
            t.setDaemon(true);
            return t;
        });

        private Channel channel;
        private final AtomicInteger responded = new AtomicInteger();
        private final AtomicInteger flushCount = new AtomicInteger();
        private final AtomicInteger maxFlushBatch = new AtomicInteger();

        void open(String host, int port) throws Exception {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LineBasedFrameDecoder(4096))
                                    .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                    .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                    .addLast(new ClientHandler());
                        }
                    });
            channel = bootstrap.connect(host, port).sync().channel();
            System.out.println("[LettuceLikeConnection] 已连接 127.0.0.1:" + port
                    + "，所有业务线程将共享这一条连接(channel)，非连接池模式");
        }

        /** 发送：并发写 → 无锁队列 → EventLoop 批量写出。任意线程可调，立即返回 future */
        CompletableFuture<String> dispatchAsync(String content) {
            Command cmd = new Command(content);
            // 无锁入队：多个业务线程同时调用也只入队，不做任何网络 IO
            pendingWrites.offer(cmd);
            // 转交 IO 线程执行 flush：若 IO 线程正忙，后续线程的命令会在队列里继续积压，
            // 待 IO 线程空闲时一次性批量写出 —— 天然形成 pipelining
            channel.eventLoop().execute(this::flushPendingWrites);
            return cmd.future;
        }

        /** sync 门面：对应 Lettuce 的 sync() API —— 阻塞的是调用线程，连接本身不阻塞 */
        String dispatchSync(String content) {
            try {
                return dispatchAsync(content).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("command failed: " + content, e);
            }
        }

        /** 只在 EventLoop(IO)线程执行：批量 drain 无锁队列 → 按入队顺序 write → 一次 flush 写出 */
        private void flushPendingWrites() {
            int n = 0;
            Command c;
            while ((c = pendingWrites.poll()) != null) {
                // 按入队顺序 write（编码由 StringEncoder 完成），写出顺序即响应匹配顺序
                channel.write(c.content + "\n");
                inFlight.addLast(c);
                n++;
            }
            if (n > 0) {
                channel.flush(); // 一次 flush 批量发出积压的全部命令，这就是 pipelining 的来源
                flushCount.incrementAndGet();
                maxFlushBatch.accumulateAndGet(n, Math::max);
                if (flushCount.get() <= 5) {
                    System.out.println("[EventLoop] " + Thread.currentThread().getName()
                            + " 本轮批量写出 " + n + " 条命令");
                }
            }
        }

        int getFlushCount() {
            return flushCount.get();
        }

        int getMaxFlushBatch() {
            return maxFlushBatch.get();
        }

        /** 对应 CommandHandler.channelRead：IO 线程按 FIFO 取队头命令完成匹配 */
        private class ClientHandler extends SimpleChannelInboundHandler<String> {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                Command head = inFlight.pollFirst();
                if (head == null) {
                    return;
                }
                head.future.complete(msg); // CompletableFuture 完成 → 阻塞在 get() 的业务线程被唤醒
                int n = responded.incrementAndGet();
                if (n <= 5) {
                    System.out.println("[IO线程] " + Thread.currentThread().getName() + " 收到响应 '" + msg
                            + "'，complete 命令 '" + head.content + "' 的 future，唤醒等待的业务线程");
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                cause.printStackTrace();
                ctx.close();
            }
        }

        @Override
        public void close() throws IOException {
            if (channel != null) {
                channel.close();
            }
            group.shutdownGracefully();
        }
    }

    // ==================== 4. 测试：多线程共享单连接发送，计数器累加，打印结果 ====================

    /**
     * 多线程共享一条连接 sync 发送：
     * ① 4 个业务线程同时起跑（CyclicBarrier 制造并发写）
     * ② 每个线程内部计数器累加 + StringBuilder 累加返回字符串
     * ③ 校验每个响应与请求一一匹配（验证 FIFO 匹配不串）
     */
    @Test
    public void testMultiThreadSyncDispatch() throws Exception {
        int threadCount = 4;
        int requestsPerThread = 5;
        try (MockRedisServer server = MockRedisServer.start();
             LettuceLikeConnection connection = new LettuceLikeConnection()) {
            connection.open("127.0.0.1", server.getPort());

            ExecutorService bizPool = Executors.newFixedThreadPool(threadCount);
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            AtomicInteger totalCounter = new AtomicInteger();
            List<Future<String>> threadResults = new ArrayList<>();
            long start = System.currentTimeMillis();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                threadResults.add(bizPool.submit((Callable<String>) () -> {
                    barrier.await(); // 同时起跑，制造多线程并发写同一连接
                    StringBuilder accumulated = new StringBuilder();
                    int counter = 0;
                    for (int i = 0; i < requestsPerThread; i++) {
                        String request = "t" + threadId + "-req" + i;
                        // 阻塞当前业务线程等待响应，但连接不阻塞：其他线程的命令照常入队、写出
                        String response = connection.dispatchSync(request);
                        // 响应回显请求内容，校验响应与请求严格一一匹配（FIFO 匹配不串的关键证据）
                        Assert.assertEquals("响应必须与请求一一匹配", request, response.split(":")[0]);
                        accumulated.append(response).append(" | ");
                        counter++;
                        totalCounter.incrementAndGet();
                    }
                    System.out.println("[业务线程] " + Thread.currentThread().getName() + " 完成，计数=" + counter
                            + "，累加结果=" + accumulated);
                    return accumulated.toString();
                }));
            }

            for (Future<String> f : threadResults) {
                Assert.assertNotNull(f.get(30, TimeUnit.SECONDS));
            }
            bizPool.shutdown();

            System.out.println("========== testMultiThreadSyncDispatch 结果 ==========");
            System.out.println("耗时: " + (System.currentTimeMillis() - start) + "ms"
                    + " (服务端串行处理 " + threadCount * requestsPerThread + " 条命令，每条 sleep " + COMMAND_COST_MS + "ms)");
            System.out.println("客户端总计数: " + totalCounter.get());
            System.out.println("服务端总处理: " + server.getProcessed());
            System.out.println("EventLoop flush 次数: " + connection.getFlushCount()
                    + "，单轮最大批量写出: " + connection.getMaxFlushBatch() + " 条 (批量>1 即 pipelining 证据)");
            Assert.assertEquals(threadCount * requestsPerThread, totalCounter.get());
            Assert.assertEquals(threadCount * requestsPerThread, server.getProcessed());
        }
    }

    /**
     * async pipelining：一个业务线程连发 10 条命令全部立即返回（不等响应），
     * 展示「发出耗时 ≈ 0ms」与服务端慢速处理解耦；随后统一等待 Future 依次被 IO 线程完成唤醒。
     */
    @Test
    public void testAsyncPipelining() throws Exception {
        int commandCount = 10;
        try (MockRedisServer server = MockRedisServer.start();
             LettuceLikeConnection connection = new LettuceLikeConnection()) {
            connection.open("127.0.0.1", server.getPort());

            long start = System.currentTimeMillis();
            List<CompletableFuture<String>> futures = new ArrayList<>();
            for (int i = 0; i < commandCount; i++) {
                futures.add(connection.dispatchAsync("pipeline-req" + i)); // 发出即返回，不阻塞
            }
            long sendCost = System.currentTimeMillis() - start;
            System.out.println("[业务线程] " + commandCount + " 条命令全部发出耗时=" + sendCost
                    + "ms（不等响应，服务端慢速串行处理中...）");

            // 统一等待：IO 线程逐条收到响应后 complete，业务线程逐个被唤醒
            for (int i = 0; i < commandCount; i++) {
                String resp = futures.get(i).get(10, TimeUnit.SECONDS);
                Assert.assertEquals("pipeline-req" + i, resp.split(":")[0]);
            }
            long totalCost = System.currentTimeMillis() - start;
            System.out.println("========== testAsyncPipelining 结果 ==========");
            System.out.println("服务端串行处理 " + commandCount + " 条(每条 sleep " + COMMAND_COST_MS + "ms) 总耗时="
                    + totalCost + "ms，发出耗时仅 " + sendCost + "ms");
            System.out.println("EventLoop flush 次数: " + connection.getFlushCount()
                    + "，单轮最大批量写出: " + connection.getMaxFlushBatch() + " 条");
            Assert.assertEquals(commandCount, server.getProcessed());
        }
    }
}
