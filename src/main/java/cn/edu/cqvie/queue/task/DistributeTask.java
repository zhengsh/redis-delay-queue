package cn.edu.cqvie.queue.task;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

import static cn.edu.cqvie.queue.RedisDelayQueue.*;

/**
 * 分发任务
 */
@Component
public class DistributeTask {

    private static final String LUA_SCRIPT;
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private StringRedisTemplate redisTemplate;

    static {
        StringBuilder sb = new StringBuilder(128);
        sb.append("local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, 1)\n");
        sb.append("if(next(val) ~= nil) then\n");
        sb.append("    redis.call('sadd', KEYS[2], ARGV[2])\n");
        sb.append("    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)\n");
        sb.append("    for i = 1, #val, 100 do\n");
        sb.append("        redis.call('rpush', KEYS[3], unpack(val, i, math.min(i+99, #val)))\n");
        sb.append("    end\n");
        sb.append("    return 1\n");
        sb.append("end\n");
        sb.append("return 0");
        LUA_SCRIPT = sb.toString();
    }

    /**
     * 2秒钟扫描一次执行队列
     */
    @Scheduled(cron = "0/5 * * * * ?")
    public void scheduledTaskByCorn() {
        try {
            Set<String> members = redisTemplate.opsForSet().members(META_TOPIC_WAIT);
            assert members != null;
            for (String k : members) {
                if (!redisTemplate.hasKey(k)) {
                    // 如果 KEY 不存在元数据中删除
                    redisTemplate.opsForSet().remove(META_TOPIC_WAIT, k);
                    continue;
                }

                String lk = k.replace("delay:wait", "delay:active");
                Object[] keys = new Object[]{serialize(k), serialize(META_TOPIC_ACTIVE), serialize(lk)};
                Object[] values = new Object[]{serialize(String.valueOf(System.currentTimeMillis())), serialize(lk)};
                Long result = redisTemplate.execute((RedisCallback<Long>) connection -> {
                    Object nativeConnection = connection.getNativeConnection();

                    if (nativeConnection instanceof RedisAsyncCommands) {
                        RedisAsyncCommands commands = (RedisAsyncCommands) nativeConnection;
                        return (Long) commands.getStatefulConnection().sync().eval(LUA_SCRIPT, ScriptOutputType.INTEGER, keys, values);
                    } else if (nativeConnection instanceof RedisAdvancedClusterAsyncCommands) {
                        RedisAdvancedClusterAsyncCommands commands = (RedisAdvancedClusterAsyncCommands) nativeConnection;
                        return (Long) commands.getStatefulConnection().sync().eval(LUA_SCRIPT, ScriptOutputType.INTEGER, keys, values);
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