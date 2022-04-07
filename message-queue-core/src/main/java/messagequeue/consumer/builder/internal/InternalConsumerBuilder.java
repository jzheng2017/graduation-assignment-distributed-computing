package messagequeue.consumer.builder.internal;

import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.springframework.stereotype.Service;

import java.util.Map;

public class InternalConsumerBuilder {
    private InternalConsumerFactory consumerFactory;
    private SubscriptionManager subscriptionManager;

    public InternalConsumerBuilder(InternalConsumerFactory consumerFactory, SubscriptionManager subscriptionManager) {
        this.consumerFactory = consumerFactory;
        this.subscriptionManager = subscriptionManager;
    }

    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        Consumer consumer = consumerFactory.createConsumer(consumerProperties);
        subscriptionManager.subscribe(consumerProperties.subscriptions(), Map.of("name", consumerProperties.name(), "consumer", consumer));
        return consumer;
    }
}
