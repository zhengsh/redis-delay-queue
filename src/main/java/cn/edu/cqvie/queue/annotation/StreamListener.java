package cn.edu.cqvie.queue.annotation;

import java.lang.annotation.*;

/**
 * 延迟队列
 *
 * @author zhengsh
 * @date 2020-03-27
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface StreamListener {

    /**
     * 监听主题
     *
     * @return
     */
    String value() default "delay:list:default";
}
