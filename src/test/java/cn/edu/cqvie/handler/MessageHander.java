package cn.edu.cqvie.handler;

import cn.edu.cqvie.queue.DelayMessage;
import cn.edu.cqvie.queue.annotation.StreamListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MessageHander {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @StreamListener
    public void handler(DelayMessage delayMessage) {
        logger.info("message hander :{}", delayMessage);
    }
}
