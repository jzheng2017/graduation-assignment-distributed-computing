package coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "coordinator"} )
public class CoordinatorMainApp {
    public static void main(String[] args) {
        SpringApplication.run(CoordinatorMainApp.class, args);
    }
}
