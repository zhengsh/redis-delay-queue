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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private ExecutorService executors = Executors.newFixedThreadPool(8);
    private Map<Object, Method> map = new HashMap<>();

    @Scheduled(cron = "0/3 * * * * ?") //每10秒执行一次
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
                    for (Map.Entry<Object, Method> entry : map.entrySet()) {
                        DelayMessage message = JSON.parseObject(s, DelayMessage.class);
                        executors.submit(() -> {
                            try {
                                entry.getValue().invoke(entry.getKey(), message);
                            } catch (Throwable t) {
                                // 失败重新放入失败队列
                                String failKey = k.replace("delay:active", "delay:fail");
                                redisTemplate.opsForList().rightPush(failKey, s);
                                logger.warn("延迟队列[3]，消息监听器发送异常: ", t);
                            }
                        });
                        logger.info("延迟队列[3]，消息到期发送到消息监听器: {}", message.getTopic());
                        break;
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private Map<Object, Method> getBean(Class<? extends Annotation> annotationClass) {
        if (!this.map.isEmpty()) {
            return this.map;
        }
        Map<Object, Method> map = new HashMap<>();
        String[] beans = applicationContext.getBeanDefinitionNames();
        for (String beanName : beans) {
            Class<?> clazz = applicationContext.getType(beanName);
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                boolean present = method.isAnnotationPresent(annotationClass);
                if (present) {
                    map.put(applicationContext.getBean(beanName), method);
                }
            }
        }
        this.map = map;
        return map;
    }
}
