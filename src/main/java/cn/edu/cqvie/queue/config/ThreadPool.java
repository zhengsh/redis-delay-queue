package cn.edu.cqvie.queue.config;

import java.util.concurrent.*;

public class ThreadPool {

    public static ThreadPoolExecutor newThreadPoolExecutor() {
        ThreadFactory threadFactory = new ThreadFactory("delay-queue");
        int nThread = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(nThread, nThread * 2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    public static ScheduledExecutorService newScheduledExecutor() {
        return new ScheduledThreadPoolExecutor(1,
                new ThreadFactory("delay-queue"));
    }

}
