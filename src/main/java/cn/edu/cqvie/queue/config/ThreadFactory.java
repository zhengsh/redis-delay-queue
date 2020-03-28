package cn.edu.cqvie.queue.config;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactory implements java.util.concurrent.ThreadFactory {

    private AtomicInteger atomic = new AtomicInteger(0);
    private String name;

    public ThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(String.format(name + "-%d", atomic.incrementAndGet()));
    }
}
