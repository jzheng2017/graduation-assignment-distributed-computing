package coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "coordinator", "datastorage"} )
@EnableScheduling
public class CoordinatorMainApp {
    public static void main(String[] args) {
        System.setProperty("consumer.statistics.publisher", "off");
        System.setProperty("heartbeat", "off");
        SpringApplication.run(CoordinatorMainApp.class, args);
    }
}
