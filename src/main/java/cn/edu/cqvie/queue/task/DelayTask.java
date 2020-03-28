package cn.edu.cqvie.queue.task;


import lombok.Setter;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.Set;

/**
 * 延迟任务处理
 */
@Setter
public class DelayTask implements Runnable {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private String topic;

    @SneakyThrows
    @Override
    public void run() {
        Set<ZSetOperations.TypedTuple<String>> set = redisTemplate.opsForZSet().rangeByScoreWithScores(this.topic,
                0, System.currentTimeMillis());
        if (set != null && set.size() > 0) {
            ZSetOperations.TypedTuple<String> s = set.iterator().next();
            String value = s.getValue();
            Long remove = redisTemplate.opsForZSet().remove(this.topic, s);
            if (remove != null && remove > 0L) {
                assert value != null;
                redisTemplate.opsForList().leftPush(this.topic, value);
            }
        }
        Thread.sleep(10L);
    }
}
