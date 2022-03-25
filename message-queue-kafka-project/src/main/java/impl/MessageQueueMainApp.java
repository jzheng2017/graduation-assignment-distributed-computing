package impl;

import messagequeue.configuration.TaskManagerProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@Import(value = {TaskManager.class, TaskManagerProperties.class})
@SpringBootApplication
public class MessageQueueMainApp {
    public static void main(String[] args) {
        SpringApplication.run(MessageQueueMainApp.class, args);
    }

}
