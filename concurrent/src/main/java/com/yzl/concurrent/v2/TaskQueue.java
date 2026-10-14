package com.yzl.concurrent.v2;

import com.yzl.concurrent.Task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务队列：内部分为拉取队列与重试队列两个非阻塞队列（{@link ConcurrentLinkedQueue}），容量只是软上限。
 * 抓取线程放入拉取队列（{@link #putFetch}），资源池满快速失败的任务放回重试队列（{@link #putRetry}），
 * 放入不阻塞、允许超过容量；取出时优先取重试队列。
 * 是否"满"由 {@link TaskFetcher} 按两个队列长度之和是否大于 {@link #fetchCapacity()} 判断，满则暂停抓取。
 * 任一队列放入任务时 offer 一个容量 1 的到达信号（重复 offer 自动合并）：
 * 消费线程等待信号期间两个队列都为空，被唤醒或超时后先重查两个队列，任一队列有任务即可取出
 */
public class TaskQueue {

    /** 拉取队列默认最大长度（"满"判定阈值：两队列长度之和大于该值即暂停抓取） */
    public static final int DEFAULT_FETCH_CAPACITY = 10;

    private final int fetchCapacity;
    /** 拉取队列：非阻塞队列，放入不阻塞、允许超过容量（软上限） */
    private final ConcurrentLinkedQueue<Task> fetchQueue = new ConcurrentLinkedQueue<>();
    /** 重试队列：非阻塞队列，放入不阻塞、允许超过容量（软上限） */
    private final ConcurrentLinkedQueue<Task> retryQueue = new ConcurrentLinkedQueue<>();
    /** 有任务到达信号：容量 1，任一队列放入任务时 offer，唤醒等待的消费线程（重复 offer 自动合并） */
    private final BlockingQueue<Boolean> signal = new ArrayBlockingQueue<>(1);
    private final AtomicInteger maxObservedSize = new AtomicInteger();

    public TaskQueue() {
        this(DEFAULT_FETCH_CAPACITY);
    }

    public TaskQueue(int fetchCapacity) {
        this.fetchCapacity = fetchCapacity;
    }

    /** 放入拉取队列，不阻塞、允许超过容量，抓取线程调用 */
    public void putFetch(Task task) {
        fetchQueue.offer(task);
        recordMaxSize();
        signal.offer(Boolean.TRUE);
    }

    /** 放入重试队列，不阻塞、允许超过容量，资源池满快速失败的任务回退时调用 */
    public void putRetry(Task task) {
        retryQueue.offer(task);
        recordMaxSize();
        signal.offer(Boolean.TRUE);
    }

    /** 非阻塞取任务：优先重试队列，都没有返回 null */
    public Task poll() {
        Task task = takeOne();
        if (task != null) {
            signal.poll();
        }
        return task;
    }

    /** 两队列都为空时阻塞等待，任一队列放入任务即唤醒；超时返回 null */
    public Task poll(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            Task task = takeOne();
            if (task != null) {
                signal.poll();
                return task;
            }
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) {
                return null;
            }
            // 等待任务到达信号，醒来后重查两队列（信号可能合并或过期）
            signal.poll(remain, TimeUnit.MILLISECONDS);
        }
    }

    /** 优先取重试队列，其次拉取队列 */
    private Task takeOne() {
        Task task = retryQueue.poll();
        if (task == null) {
            task = fetchQueue.poll();
        }
        return task;
    }

    /** 拉取队列长度 */
    public int fetchSize() {
        return fetchQueue.size();
    }

    /** 重试队列长度 */
    public int retrySize() {
        return retryQueue.size();
    }

    /** 两队列任务总数 */
    public int size() {
        return fetchQueue.size() + retryQueue.size();
    }

    public boolean isEmpty() {
        return fetchQueue.isEmpty() && retryQueue.isEmpty();
    }

    /** 拉取队列最大长度，"满"判定阈值 */
    public int fetchCapacity() {
        return fetchCapacity;
    }

    /** 两队列总长度的历史峰值 */
    public int getMaxObservedSize() {
        return maxObservedSize.get();
    }

    private void recordMaxSize() {
        int now = fetchQueue.size() + retryQueue.size();
        int prev;
        do {
            prev = maxObservedSize.get();
            if (now <= prev) {
                return;
            }
        } while (!maxObservedSize.compareAndSet(prev, now));
    }
}
