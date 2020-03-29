package cn.edu.cqvie.queue.impl;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.RedisDelayQueue;
import com.alibaba.fastjson.JSON;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

/**
 * 延迟队列实现
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Component
public class RedisDelayQueueImpl<E extends DelayMessage> implements RedisDelayQueue<E> {
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
            String u =
                    "redis.call('sadd', KEYS[1], ARGV[1])\n" +
                            "redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])\n" +
                            "return 1";

            Object[] keys = new Object[]{serialize(META_TOPIC_WAIT), serialize(zkey)};
            Object[] values = new Object[]{ serialize(zkey), serialize(String.valueOf(e.getDelayTime())),serialize(jsonStr)};

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
            logger.info("延迟队列[1]，消息推送成功进入等待队列({}), topic: {}", result != null && result > 0, e.getTopic());
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
