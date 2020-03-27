package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * 延迟队列实现
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Component
public class RedisDelayQueueImpl<E extends DelayMessage> extends AbstractRedisDelayQueue<E> {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private String channel;

    @Override
    public void poll() {

    }

    /**
     * 发送消息
     *
     * @param e
     */
    @Override
    public void push(E e) {
        DelayMessage message = e;
        String v = JSON.toJSONString(e);
        this.channel = message.getChannel();
        redisTemplate.opsForZSet().add(message.getChannel(), v, e.getDelayTime());
    }

    /**
     * 循环读取
     */
    @SneakyThrows
    public void loop() {
        for (; ; ) {
            Set<ZSetOperations.TypedTuple<String>> set = redisTemplate.opsForZSet().rangeByScoreWithScores(this.channel,
                    0, System.currentTimeMillis());
            if (set != null && set.size() > 0) {
                ZSetOperations.TypedTuple<String> s = set.iterator().next();
                String value = s.getValue();
                Long remove = redisTemplate.opsForZSet().remove(this.channel, s);
                if (remove != null && remove > 0L) {
                    assert value != null;
                    redisTemplate.opsForList().leftPush(this.channel, value);
                }
            }
            Thread.sleep(10L);
        }
    }

}
