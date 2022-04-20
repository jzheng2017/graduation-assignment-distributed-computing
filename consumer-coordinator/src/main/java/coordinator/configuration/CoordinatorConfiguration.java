package coordinator.configuration;

import datastorage.KVClient;
import messagequeue.configuration.EnvironmentSetup;
import messagequeue.consumer.ConsumerManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CoordinatorConfiguration {
    @Bean
    public EnvironmentSetup environmentSetup(ConsumerManager consumerManager, KVClient kvClient) {
        return new EnvironmentSetup(
                false,
                consumerManager,
                kvClient);
    }
}
