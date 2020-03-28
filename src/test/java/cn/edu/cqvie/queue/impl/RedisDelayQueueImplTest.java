package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.RedisDelayQueue;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;


@SpringBootTest
class RedisDelayQueueImplTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService executors = Executors.newScheduledThreadPool(4);
    private ScheduledExecutorService scheduledExecutors = Executors.newScheduledThreadPool(4);

    @Autowired
    private RedisDelayQueue redisDelayQueue;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @SneakyThrows
    @Test
    void push() {
        for (int i = 0; i < 10; i++) {
            executors.submit(() -> {
                DelayMessage message = new DelayMessage();
                message.setDelayTime(System.currentTimeMillis() + 3000L);
                redisDelayQueue.push(message);
            });
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executors.shutdown();
        while (true) {
            Thread.sleep(1000L);
        }
    }
}