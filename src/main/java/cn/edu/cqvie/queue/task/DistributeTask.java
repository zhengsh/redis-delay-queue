package cn.edu.cqvie.queue.task;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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

    @Scheduled(cron = "0/2 * * * * ?")
    public void scheduledTaskByCorn() {
        try {
            Set<String> members = redisTemplate.opsForSet().members(META_TOPIC_WAIT);
            for (String k : members) {
                if (!redisTemplate.hasKey(k)) {
                    // 如果 KEY 不存在元数据中删除
                    redisTemplate.opsForSet().remove(META_TOPIC_WAIT, k);
                    continue;
                }

                String u =
                        "local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, 1)\n" +
                        "if(next(val) ~= nil) then\n" +
                        //"    redis.call('sadd', KEYS[2], ARGV[2])\n" +
                        // 移除有序集中，指定排名(rank)区间内的所有成员
                        "    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)\n" +
                        "    for i = 1, #val, 100 do\n" +
                        "        redis.call('publish', KEYS[2], unpack(val, i, math.min(i+99, #val)))\n" +
                        "    end\n" +
                        "    return 1\n" +
                        "end\n" +
                        "return 0";


                Object[] keys = new Object[]{serialize(k), serialize(TOPIC_ACTIVE)};
                Object[] values = new Object[]{serialize(String.valueOf(System.currentTimeMillis()))};
                Long result = redisTemplate.execute((RedisCallback<Long>) connection -> {
                    Object nativeConnection = connection.getNativeConnection();

                    if (nativeConnection instanceof RedisAsyncCommands) {
                        RedisAsyncCommands commands = (RedisAsyncCommands) nativeConnection;
                        return (Long) commands.getStatefulConnection().sync().eval(u, ScriptOutputType.INTEGER, keys, values);
                    } else if (nativeConnection instanceof RedisAdvancedClusterAsyncCommands) {
                        RedisAdvancedClusterAsyncCommands commands = (RedisAdvancedClusterAsyncCommands) nativeConnection;
                        return (Long) commands.getStatefulConnection().sync().eval(u, ScriptOutputType.INTEGER, keys, values);
                    }
                    return 0L;
                });
                logger.info("延迟队列[2]，消息到期进入执行队列({}): {}", result != null && result > 0, TOPIC_ACTIVE);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private byte[] serialize(String key) {
        RedisSerializer<String> stringRedisSerializer =
                (RedisSerializer<String>) redisTemplate.getKeySerializer();
        //lettuce连接包下序列化键值，否则无法用默认的ByteArrayCodec解析
        return stringRedisSerializer.serialize(key);
    }

}