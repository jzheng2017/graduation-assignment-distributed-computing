package worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "worker", "datastorage"})
@EnableScheduling
public class WorkerMainApp {
    public static void main(String[] args) throws InterruptedException {
        SpringApplication app = new SpringApplication(WorkerMainApp.class);
        app.setDefaultProperties(Collections.singletonMap("server.port", "8081"));
        app.run(args);
//        SpringApplication.run(MessageQueueMainApp.class);
    }
}

