package kafka.subscription;

import kafka.consumer.KafkaConsumer;
import messagequeue.messagebroker.subscription.Subscription;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is an implementation of the {@link SubscriptionManager} interface and allows to subscribe consumers to topics.
 */
@Service
public class KafkaSubscriptionManager implements SubscriptionManager {
    private Logger logger = LoggerFactory.getLogger(KafkaSubscriptionManager.class);

    @Override
    public void subscribe(Set<String> topicList, Map<String, Object> consumerContext) {
        final messagequeue.consumer.Consumer consumer = ((messagequeue.consumer.Consumer) consumerContext.get("consumer"));
        final Consumer<String, String> kafkaConsumer = getConsumer(consumerContext);
        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.addAll(topicList);
        kafkaConsumer.subscribe(subscriptions, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //do nothing
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                collection.forEach(topicPartition -> kafkaConsumer.seek(
                                topicPartition,
                                consumer.getTopicOffset(topicPartition.topic())
                        )
                );
            }
        });
        logger.info("Updated subscription list of consumer {}", consumer.getIdentifier());
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public void unsubscribe(Set<String> topicList, Map<String, Object> consumerContext) {
        final messagequeue.consumer.Consumer consumer = ((messagequeue.consumer.Consumer) consumerContext.get("consumer"));

        final Consumer<String, String> kafkaConsumer = getConsumer(consumerContext);

        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.removeAll(topicList);
        kafkaConsumer.subscribe(subscriptions); //the way this function works is that regardless if you want to subscribe or unsubscribe, you use this function, and it will update the subscriptions based on the list.
        logger.info("Updated subscription list of consumer {}", consumer.getIdentifier());
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public Set<Subscription> getSubscriptions(Map<String, Object> consumerContext) {
        final messagequeue.consumer.Consumer consumer = ((messagequeue.consumer.Consumer) consumerContext.get("consumer"));
        final Consumer<String, String> kafkaConsumer = getConsumer(consumerContext);

        logger.info("Retrieving subscription list of consumer {}", consumer.getIdentifier());
        return kafkaConsumer.subscription().stream().map(Subscription::new).collect(Collectors.toSet());
    }

    @Override
    public boolean isSubscribed(String topicName, Map<String, Object> consumerContext) {
        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());

        return subscriptions.contains(topicName);
    }

    private Consumer<String, String> getConsumer(Map<String, Object> consumerContext) {
        if (consumerContext.containsKey("consumer")) {
            return ((KafkaConsumer) consumerContext.get("consumer")).getConsumer();
        }

        throw new IllegalStateException("No consumer was provided");
    }
}
