package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

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
            // todo 一下两个操作需要保证一致性
            redisTemplate.opsForSet().add(META_TOPIC_WAIT, zkey);
            redisTemplate.opsForZSet().add(zkey, jsonStr, e.getDelayTime());
            logger.info("延迟队列[1]，消息推送成功进入等待队列, topic: {}", e.getTopic());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
