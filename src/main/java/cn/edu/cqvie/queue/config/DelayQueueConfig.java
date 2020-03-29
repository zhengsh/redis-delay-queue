//package cn.edu.cqvie.queue.config;
//
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.data.redis.connection.RedisConnectionFactory;
//import org.springframework.data.redis.listener.PatternTopic;
//import org.springframework.data.redis.listener.RedisMessageListenerContainer;
//import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
//
//import java.util.concurrent.CountDownLatch;
//
//import static cn.edu.cqvie.queue.RedisDelayQueue.TOPIC_ACTIVE;
//
//@Configuration
//public class DelayQueueConfig {
//
//    @Autowired
//    private TaskActiveReceiver receiver;
//
//    @Bean
//    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
//                                            MessageListenerAdapter listenerAdapter) {
//
//        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
//        container.setConnectionFactory(connectionFactory);
//        container.addMessageListener(listenerAdapter, new PatternTopic(TOPIC_ACTIVE));
//        return container;
//    }
//
//    @Bean
//    public MessageListenerAdapter listenerAdapter() {
//        return new MessageListenerAdapter(receiver, "receiveMessage");
//    }
//
//}
