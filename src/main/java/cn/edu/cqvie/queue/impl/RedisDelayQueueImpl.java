package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import com.alibaba.fastjson.JSON;
import lombok.Setter;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 延迟队列实现
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Component
public class RedisDelayQueueImpl<E extends DelayMessage> extends AbstractRedisDelayQueue<E> {

    private ExecutorService pool;

    @Autowired
    private StringRedisTemplate redisTemplate;

    {
        ThreadFactory threadFactory = new ThreadFactory() {
            AtomicInteger atomic = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(String.format("queue-pool-%d", atomic.incrementAndGet()));
            }
        };
        int nThread = Runtime.getRuntime().availableProcessors();
        pool = new ThreadPoolExecutor(nThread, nThread * 2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void poll() {
        // todo
    }

    /**
     * 发送消息
     *
     * @param e
     */
    @Override
    public void push(E e) {
        String jsonStr = JSON.toJSONString(e);
        String topic = e.getTopic();
        String zkey = String.format("delay:queue:%", topic);
        redisTemplate.opsForZSet().add(zkey, jsonStr, e.getDelayTime());
    }

    /**
     * 延迟任务处理
     */
    @Setter
    public class DelayTask implements Runnable {

        private String topic;

        @SneakyThrows
        @Override
        public void run() {
            for (; ; ) {
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
    }

    @PreDestroy
    private void destroy () {
        pool.shutdown();
    }
}
