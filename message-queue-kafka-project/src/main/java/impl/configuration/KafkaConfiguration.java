package impl.configuration;

import datastorage.KVClient;
import messagequeue.configuration.EnvironmentSetup;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

    @Bean
    public ConsumerBuilder consumerBuilder(ConsumerConfigurationParser consumerConfigurationParser, ConsumerFactory consumerFactory, SubscriptionManager subscriptionManager) {
        return new ConsumerBuilder(consumerConfigurationParser, consumerFactory, subscriptionManager);
    }

    @Bean
    public EnvironmentSetup environmentSetup(ConsumerManager consumerManager, KVClient kvClient) {
        return new EnvironmentSetup(true, consumerManager, kvClient);
    }
}
