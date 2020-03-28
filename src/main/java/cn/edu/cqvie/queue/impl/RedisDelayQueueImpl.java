package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * 延迟队列实现
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Component
public class RedisDelayQueueImpl<E extends DelayMessage> extends AbstractRedisDelayQueue<E> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public void poll() {
        // todo
    }

    /**
     * 发送消息
     *
     * @param e
     */
    @SneakyThrows
    @Override
    public void push(E e) {
        try {
            String jsonStr = JSON.toJSONString(e);
            String topic = e.getTopic();
            String zkey = String.format("delay:wait:%s", topic);
            // 存入元数据
            redisTemplate.opsForSet().add(META_TOPIC, zkey);
            // 存消息内容
            redisTemplate.opsForZSet().add(zkey, jsonStr, e.getDelayTime());
            logger.info("delay-queue-push, topic: {}", e.getTopic());
            Thread.sleep(1000);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
