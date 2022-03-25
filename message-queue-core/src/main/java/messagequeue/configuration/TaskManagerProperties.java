package messagequeue.configuration;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TaskManagerProperties {
    private int threadPoolSize = 5;

    public int getThreadPoolSize() {
        return threadPoolSize;
    }
}
