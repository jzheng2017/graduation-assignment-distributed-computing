package messagequeue.messagebroker.subscription;

import java.util.Map;
import java.util.Set;

/**
 * An interface allows one to manage topic subscriptions
 */
public interface SubscriptionManager {

    /**
     * Subscribe to a set of topics
     *
     * @param topicList a set of topic names
     */
    void subscribe(Set<String> topicList, Map<String, Object> consumerContext);

    /**
     * Unsubscribe from a set of topics
     * @param topicList a set of topic names
     */
    void unsubscribe(Set<String> topicList, Map<String, Object> consumerContext);

    /**
     * Get all topics that the subscriber is subscribed to
     *
     * @return a list of topic subscriptions
     */
    Set<Subscription> getSubscriptions(Map<String, Object> consumerContext);

    /**
     * Determine whether the subscriber is subscribed to a given topic
     *
     * @param topicName the name of the topic
     * @return true if it is subscribed to the topic, false otherwise
     */
    boolean isSubscribed(String topicName, Map<String, Object> consumerContext);
}
