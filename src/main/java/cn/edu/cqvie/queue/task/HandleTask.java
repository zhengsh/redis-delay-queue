package cn.edu.cqvie.queue.task;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.annotation.StreamListener;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import static cn.edu.cqvie.queue.RedisDelayQueue.META_TOPIC_ACTIVE;

/**
 * 任务处理器
 */
@Component
public class HandleTask {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ApplicationContext applicationContext;

    @Scheduled(cron = "0/1 * * * * ?") //每10秒执行一次
    public void scheduledTaskByCorn() {
        try {
            Set<String> members = redisTemplate.opsForSet().members(META_TOPIC_ACTIVE);
            Map<Object, Method> map = getBean(StreamListener.class);
            for (String k : members) {
                if (!redisTemplate.hasKey(k)) {
                    // 如果 KEY 不存在元数据中删除
                    redisTemplate.opsForSet().remove(META_TOPIC_ACTIVE, k);
                    continue;
                }
                String s = redisTemplate.opsForList().leftPop(k);
                if (s != null && !"".equals(s.trim())) {
                    logger.info("redis key : {}", k);
                    for (Map.Entry<Object, Method> entry : map.entrySet()) {
                        DelayMessage message = JSON.parseObject(s, DelayMessage.class);
                        entry.getValue().invoke(entry.getKey(), message);
                        logger.info("延迟队列[3]，消息到期发送到消息监听器: {}", message);
                        break;
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private Map<Object, Method> getBean(Class<? extends Annotation> annotationClass) {
        Map<Object, Method> map = new HashMap<>();
        String[] beans = applicationContext.getBeanDefinitionNames();
        for (String beanName : beans) {
            if ("messageHander".equals(beanName)) {
                logger.info("debug code");
            }
            Class<?> clazz = applicationContext.getType(beanName);
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                boolean present = method.isAnnotationPresent(annotationClass);
                if (present) {
                    map.put(applicationContext.getBean(beanName), method);
                }
            }
        }
        return map;
    }
}
