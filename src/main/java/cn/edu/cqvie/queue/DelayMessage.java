package cn.edu.cqvie.queue;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * 消息体
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Setter
@Getter
public class DelayMessage {
    /**
     * 消息唯一标识
     */
    private String id;
    /**
     * 消息主题
     */
    private String topic = "default";
    /**
     * 具体消息 json
     */
    private String body;
    /**
     * 延时时间, 格式为时间戳: 当前时间戳 + 实际延迟毫秒数
     */
    private Long delayTime = System.currentTimeMillis() + 30000L;
    /**
     * 消息发送时间
     */
    private LocalDateTime createTime;
}