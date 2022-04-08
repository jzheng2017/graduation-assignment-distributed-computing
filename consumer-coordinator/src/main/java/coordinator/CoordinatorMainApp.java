package coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "coordinator"} )
@EnableScheduling
public class CoordinatorMainApp {
    public static void main(String[] args) {
        System.setProperty("consumer.statistics.publisher", "off");
        SpringApplication.run(CoordinatorMainApp.class, args);
    }
}
