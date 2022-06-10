package coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"coordinator", "datastorage", "commons"})
@EnableScheduling
public class CoordinatorMainApp {
    public static void main(String[] args) {
        SpringApplication.run(CoordinatorMainApp.class, args);
    }
}
