package messagequeue.consumer.builder;

import messagequeue.consumer.Consumer;
import commons.ConsumerProperties;
import messagequeue.messagebroker.subscription.SubscriptionManager;

import java.util.Map;

/**
 * A builder class for building consumers
 */
public class ConsumerBuilder {
    private ConsumerConfigurationParser consumerConfigurationParser;
    private ConsumerFactory consumerFactory;
    private SubscriptionManager subscriptionManager;

    public ConsumerBuilder(ConsumerConfigurationParser consumerConfigurationParser, ConsumerFactory consumerFactory, SubscriptionManager subscriptionManager) {
        this.consumerConfigurationParser = consumerConfigurationParser;
        this.consumerFactory = consumerFactory;
        this.subscriptionManager = subscriptionManager;
    }

    public Consumer createConsumer(String consumerConfiguration) {
        ConsumerProperties consumerProperties = consumerConfigurationParser.parse(consumerConfiguration);
        Consumer consumer = consumerFactory.createConsumer(consumerProperties);
        subscriptionManager.subscribe(consumerProperties.subscriptions(), Map.of("name", consumerProperties.name(), "consumer", consumer));
        return consumer;
    }
}