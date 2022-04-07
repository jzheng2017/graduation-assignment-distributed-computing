package coordinator.configuration;

import coordinator.processor.builder.KafkaCoordinatorInternalConsumerFactory;
import messagequeue.configuration.EnvironmentSetup;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.builder.internal.InternalConsumerBuilder;
import messagequeue.consumer.builder.internal.InternalConsumerFactory;
import messagequeue.messagebroker.MessageBrokerProxy;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import messagequeue.messagebroker.topic.TopicManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CoordinatorConfiguration {

    @Bean
    public InternalConsumerBuilder internalConsumerBuilder(KafkaCoordinatorInternalConsumerFactory kafkaCoordinatorInternalConsumerFactory, SubscriptionManager subscriptionManager) {
        return new InternalConsumerBuilder(kafkaCoordinatorInternalConsumerFactory, subscriptionManager);
    }

    @Bean
    public EnvironmentSetup environmentSetup(ConsumerManager consumerManager,
                                             InternalConsumerBuilder consumerBuilder,
                                             MessageBrokerProxy messageBrokerProxy,
                                             TopicManager topicManager) {
        return new EnvironmentSetup(
                false,
                consumerManager,
                consumerBuilder,
                messageBrokerProxy,
                topicManager);
    }
}
