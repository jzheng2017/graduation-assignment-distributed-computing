package impl;

import datastorage.EtcdLockClient;
import datastorage.LockClient;
import datastorage.configuration.EtcdProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;

@SpringBootApplication(scanBasePackages = {"messagequeue", "kafka", "impl", "datastorage"})
@EnableScheduling
public class MessageQueueMainApp {
    public static void main(String[] args) throws InterruptedException {
//        SpringApplication app = new SpringApplication(MessageQueueMainApp.class);
//        app.setDefaultProperties(Collections.singletonMap("server.port", "8081"));
//        app.run(args);
//        SpringApplication.run(MessageQueueMainApp.class);
        EtcdProperties etcdProperties = new EtcdProperties();
        etcdProperties.setBaseUrl("http://localhost:2379");
        LockClient lockClient = new EtcdLockClient(etcdProperties);
        lockClient.lock("firstLock");
    }
}

