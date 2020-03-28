package cn.edu.cqvie.queue.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Set;

import static cn.edu.cqvie.queue.RedisDelayQueue.*;

/**
 * 分发任务
 */
@Component
public class DistributeTask {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Scheduled(cron = "0/1 * * * * ?") //每10秒执行一次
    public void scheduledTaskByCorn() {
        try {
            Set<String> members = redisTemplate.opsForSet().members(META_TOPIC_WAIT);
            for (String k : members) {
                if (!redisTemplate.hasKey(k)) {
                    // 如果 KEY 不存在元数据中删除
                    redisTemplate.opsForSet().remove(META_TOPIC_WAIT, k);
                    continue;
                }
                Set tuples = redisTemplate.opsForZSet().rangeByScoreWithScores(k,
                        0, System.currentTimeMillis());
                Iterator<ZSetOperations.TypedTuple<Object>> iterator = tuples.iterator();
                while (iterator.hasNext()) {
                    ZSetOperations.TypedTuple<Object> typedTuple = iterator.next();
                    Object v = typedTuple.getValue();
                    if (redisTemplate.opsForZSet().remove(k, v) > 0) {
                        String lk = String.format("delay:active:%s", k);
                        redisTemplate.opsForSet().add(META_TOPIC_ACTIVE, lk);
                        redisTemplate.opsForList().leftPush(lk, (String) v);
                        logger.info("延迟队列，消息到期进入执行队列: {}", lk);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}