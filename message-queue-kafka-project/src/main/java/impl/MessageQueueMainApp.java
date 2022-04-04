package impl;

import messagequeue.consumer.ConsumerStatisticsPublisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;

@SpringBootApplication
@EnableScheduling
@Import(value = {ConsumerStatisticsPublisher.class})
public class MessageQueueMainApp {
    public static void main(String[] args) {
//        SpringApplication app = new SpringApplication(MessageQueueMainApp.class);
//        app.setDefaultProperties(Collections.singletonMap("server.port", "8081"));
//        app.run(args);
        SpringApplication.run(MessageQueueMainApp.class);
    }
}
