package cn.edu.cqvie.queue;

/**
 * 延迟队列
 *
 * @author zhengsh
 * @date 2020-03-27
 */
public interface RedisDelayQueue<E extends DelayMessage> {
    /**
     * 拉取消息
     */
    void poll();

    /**
     * 推送延迟消息
     *
     * @param e
     */
    void push(E e);
}
