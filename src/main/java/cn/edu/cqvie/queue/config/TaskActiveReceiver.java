//package cn.edu.cqvie.queue.config;
//
//import cn.edu.cqvie.queue.DelayMessage;
//import cn.edu.cqvie.queue.annotation.StreamListener;
//import com.alibaba.fastjson.JSON;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.ApplicationContext;
//import org.springframework.stereotype.Component;
//
//import java.lang.annotation.Annotation;
//import java.lang.reflect.Method;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import static cn.edu.cqvie.queue.RedisDelayQueue.TOPIC_ACTIVE;
//
//@Component
//public class TaskActiveReceiver {
//
//    private Logger logger = LoggerFactory.getLogger(getClass());
//    @Autowired
//    private ApplicationContext applicationContext;
//    private ExecutorService executors = Executors.newFixedThreadPool(8);
//    Map<Object, Method> map = new HashMap<>();
//
//    public void receiveMessage(String message) {
//        try {
//            String topic = TOPIC_ACTIVE;
//            if (message != null && !"".equals(message.trim())) {
//                for (Map.Entry<Object, Method> entry : getBean(StreamListener.class).entrySet()) {
//                    DelayMessage m = JSON.parseObject(message, DelayMessage.class);
//                    executors.submit(() -> {
//                        try {
//                            entry.getValue().invoke(entry.getKey(), m);
//                        } catch (Throwable t) {
//                            logger.warn("延迟队列[3]，消息监听器发送异常: ", t);
//                        }
//                    });
//                    logger.info("延迟队列[3]，消息到期发送到消息监听器: {}", topic);
//                    break;
//                }
//            }
//
//        } catch (Throwable t) {
//            t.printStackTrace();
//        }
//    }
//
//    private Map<Object, Method> getBean(Class<? extends Annotation> annotationClass) {
//        if (!this.map.isEmpty()) {
//            return this.map;
//        }
//        Map<Object, Method> map = new HashMap<>();
//        String[] beans = applicationContext.getBeanDefinitionNames();
//        for (String beanName : beans) {
//            Class<?> clazz = applicationContext.getType(beanName);
//            Method[] methods = clazz.getMethods();
//            for (Method method : methods) {
//                boolean present = method.isAnnotationPresent(annotationClass);
//                if (present) {
//                    map.put(applicationContext.getBean(beanName), method);
//                }
//            }
//        }
//        this.map = map;
//        return map;
//    }
//}
