package impl.configuration;

import kafka.consumer.KafkaConsumerBuilderHelper;
import kafka.consumer.builder.KafkaInternalConsumerFactory;
import messagequeue.configuration.EnvironmentSetup;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.builder.internal.InternalConsumerBuilder;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.MessageBrokerProxy;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import messagequeue.messagebroker.topic.TopicManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

    @Bean
    public ConsumerBuilder consumerBuilder(ConsumerConfigurationParser consumerConfigurationParser, ConsumerFactory consumerFactory, SubscriptionManager subscriptionManager) {
        return new ConsumerBuilder(consumerConfigurationParser, consumerFactory, subscriptionManager);
    }

    @Bean
    public KafkaInternalConsumerFactory kafkaInternalConsumerFactory(KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper, ConsumerManager consumerManager, ConsumerBuilder consumerBuilder, TaskManager taskManager) {
        return new KafkaInternalConsumerFactory(kafkaConsumerBuilderHelper, consumerManager, consumerBuilder, taskManager);
    }

    @Bean
    public InternalConsumerBuilder internalConsumerBuilder(KafkaInternalConsumerFactory kafkaInternalConsumerFactory, SubscriptionManager subscriptionManager) {
        return new InternalConsumerBuilder(kafkaInternalConsumerFactory, subscriptionManager);
    }

    @Bean
    public EnvironmentSetup environmentSetup(ConsumerManager consumerManager, InternalConsumerBuilder internalConsumerBuilder, MessageBrokerProxy messageBrokerProxy, TopicManager topicManager) {
        return new EnvironmentSetup(true, consumerManager, internalConsumerBuilder, messageBrokerProxy, topicManager);
    }
}
