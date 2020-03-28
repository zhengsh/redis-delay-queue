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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 分发任务
 */
@Component
public class DistributeTask {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private StringRedisTemplate redisTemplate;
    private ScheduledExecutorService scheduledExecutors = Executors.newScheduledThreadPool(4);

    @Scheduled(cron = "0/1 * * * * ?") //每10秒执行一次
    public void scheduledTaskByCorn() {
        scheduledExecutors.scheduleWithFixedDelay(() -> {
            try {
                String zk = "delay:zset:default";
                Set tuples = redisTemplate.opsForZSet().rangeByScoreWithScores(zk,
                        0, System.currentTimeMillis());
                Iterator<ZSetOperations.TypedTuple<Object>> iterator = tuples.iterator();
                while (iterator.hasNext()) {
                    ZSetOperations.TypedTuple<Object> typedTuple = iterator.next();
                    Object v = typedTuple.getValue();
                    if (redisTemplate.opsForZSet().remove(zk, v) > 0) {
                        redisTemplate.opsForList().leftPush("delay:list:default", (String) v);
                    }
                    logger.info("value: {}, score: {}", typedTuple.getValue(), typedTuple.getScore());
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 3, 1, TimeUnit.SECONDS);
    }

}