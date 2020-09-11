package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.RedisDelayQueue;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.*;


@SpringBootTest
class RedisDelayQueueImplTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService executors = Executors.newScheduledThreadPool(4);

    @Autowired
    private RedisDelayQueue redisDelayQueue;

    @SneakyThrows
    @Test
    void push() {
        for (int i = 0; i < 5; i++) {
            final int topic = i;
            executors.submit(() -> {
                DelayMessage message = new DelayMessage();
                message.setTopic(String.valueOf(topic));
                message.setDelayTime(System.currentTimeMillis() + 30000L);
                logger.info("延迟队列[0]，消息推送开始: {}", message.getTopic());
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
