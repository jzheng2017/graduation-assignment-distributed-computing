package coordinator;

import datastorage.EtcdKVClient;
import datastorage.EtcdLockClient;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.EtcdProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"coordinator", "datastorage"})
@EnableScheduling
public class CoordinatorMainApp {
    public static void main(String[] args) {
        SpringApplication.run(CoordinatorMainApp.class, args);
    }
}
