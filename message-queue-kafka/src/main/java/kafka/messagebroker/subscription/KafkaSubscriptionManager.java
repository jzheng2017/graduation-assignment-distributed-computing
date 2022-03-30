package kafka.messagebroker.subscription;

import kafka.consumer.KafkaConsumer;
import messagequeue.messagebroker.subscription.Subscription;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class KafkaSubscriptionManager implements SubscriptionManager {
    private Logger logger = LoggerFactory.getLogger(KafkaSubscriptionManager.class);

    @Override
    public void subscribe(Set<String> topicList, Map<String, Object> consumerContext) {
        final Consumer<String, String> consumer = getConsumer(consumerContext);
        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.addAll(topicList); //if there are duplicate subscriptions, then it's fine as they get left out and only leaving one in. the user doesn't need to know that as it won't have any effect on the subscription process.
        consumer.subscribe(subscriptions);
        logger.info("Updated subscription list of consumer {}", consumerContext.get("name"));
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public void unsubscribe(Set<String> topicList, Map<String, Object> consumerContext) {
        final Consumer<String, String> consumer = getConsumer(consumerContext);

        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.removeAll(topicList);
        consumer.subscribe(subscriptions); //the way this function works is that regardless if you want to subscribe or unsubscribe, you use this function, and it will update the subscriptions based on the list.
        logger.info("Updated subscription list of consumer {}", consumerContext.get("name"));
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public Set<Subscription> getSubscriptions(Map<String, Object> consumerContext) {
        final Consumer<String, String> consumer = getConsumer(consumerContext);

        logger.info("Retrieving subscription list of consumer {}", consumerContext.get("name"));
        return consumer.subscription().stream().map(Subscription::new).collect(Collectors.toSet());
    }

    @Override
    public boolean isSubscribed(String topicName, Map<String, Object> consumerContext) {
        Set<String> subscriptions = getSubscriptions(consumerContext).stream().map(Subscription::topicName).collect(Collectors.toSet());

        return subscriptions.contains(topicName);
    }

    private Consumer<String, String> getConsumer(Map<String, Object> consumerContext) {
        if (consumerContext.containsKey("consumer")) {
            return ((KafkaConsumer)consumerContext.get("consumer")).getConsumer();
        }

        throw new IllegalStateException("No consumer was provided");
    }
}
