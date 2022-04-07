package coordinator.configuration;

import coordinator.ConsumerCoordinator;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.builder.ConsumerBuilder;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class CoordinatorApplicationStartupHandler implements ApplicationRunner {
    private ConsumerCoordinator consumerCoordinator;

    public CoordinatorApplicationStartupHandler(ConsumerCoordinator consumerCoordinator) {
        this.consumerCoordinator = consumerCoordinator;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        new Thread(() -> consumerCoordinator.deploy()).start();
    }
}
