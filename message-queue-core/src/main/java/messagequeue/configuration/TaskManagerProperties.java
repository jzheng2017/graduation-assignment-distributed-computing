package messagequeue.configuration;

import org.springframework.context.annotation.Configuration;

@Configuration
public class TaskManagerProperties {
    private int threadPoolSize = 50;

    public int getThreadPoolSize() {
        return threadPoolSize;
    }
}
