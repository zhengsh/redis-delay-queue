package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.RedisDelayQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
class RedisDelayQueueImplTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService executors = Executors.newScheduledThreadPool(32);
    @Autowired
    private RedisDelayQueue redisDelayQueue;

    @Test
    void push() {
        for (int i = 0; i < 1000; i++) {
            executors.submit(() -> {
                DelayMessage message = new DelayMessage();
                message.setTopic("delay-topic");
                message.setDelayTime(System.currentTimeMillis() + 30000L);
                redisDelayQueue.push(message);
            });
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}