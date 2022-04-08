package impl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "impl"})
@EnableScheduling
public class MessageQueueMainApp {
    public static void main(String[] args) {
//        SpringApplication app = new SpringApplication(MessageQueueMainApp.class);
//        app.setDefaultProperties(Collections.singletonMap("server.port", "8081"));
//        app.run(args);
        SpringApplication.run(MessageQueueMainApp.class);
    }
}

