package cn.edu.cqvie.queue.task;

import cn.edu.cqvie.queue.config.ThreadPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.edu.cqvie.queue.config.ThreadPool.newScheduledExecutor;

@Configuration
public class TaskConfiguration {

    @PostConstruct
    private void schedule() {
        ScheduledExecutorService service = newScheduledExecutor();
        DelayTask task = new DelayTask();
        service.scheduleAtFixedRate(task, 500, 500, TimeUnit.MILLISECONDS);
    }
}
