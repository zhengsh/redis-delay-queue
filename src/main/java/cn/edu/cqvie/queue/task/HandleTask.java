package cn.edu.cqvie.queue.task;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.annotation.StreamListener;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private ScheduledExecutorService scheduledExecutors = Executors.newScheduledThreadPool(4);

    @Scheduled(cron = "0/1 * * * * ?") //每10秒执行一次
    public void scheduledTaskByCorn() {
        scheduledExecutors.scheduleWithFixedDelay(() -> {
            try {
                String lk = "delay:list:default";
                String s = redisTemplate.opsForList().leftPop(lk);
                if (s != null && !"".equals(s.trim())) {
                    logger.info("redis key : {}", lk);
                    String[] beans = applicationContext.getBeanDefinitionNames();
                    for (String beanName : beans) {
                        if ("messageHander".equals(beanName))  {
                            logger.info("debug code");
                        }
                        Class<?> clazz = applicationContext.getType(beanName);
                        Method[] methods = clazz.getMethods();
                        for (Method method : methods) {
                            boolean present = method.isAnnotationPresent(StreamListener.class);
                            if (present) {
                                // 调用方法
                                DelayMessage message = JSON.parseObject(s, DelayMessage.class);
                                method.invoke(applicationContext.getBean(beanName), message);
                                return;
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 3, 1, TimeUnit.SECONDS);
    }

//    @Component
//    public class SpringBeanUtils implements ApplicationContextAware {
//        private static ApplicationContext applicationContext;
//
//        public void setApplicationContext(ApplicationContext arg0)
//                throws BeansException {
//            applicationContext = arg0;
//        }
//
//        public static <T> T getBean(String id, Class<T> clasz) {
//
//            return applicationContext.getBean(id, clasz);
//        }
//    }
}
