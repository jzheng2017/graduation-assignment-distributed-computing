package messagequeue.messagebroker.subscription;

import java.util.Set;

/**
 * This interface allows a class to be a subscriber to a topic
 */
public interface Subscriber {
    /**
     * Subscribe to a topic
     *
     * @param topicName the name of the topic
     */
    void subscribe(String topicName);

    /**
     * Subscribe to a set of topics
     *
     * @param topicList a set of topic names
     */
    void subscribe(Set<String> topicList);

    /**
     * Unsubscribe to a topic
     *
     * @param topicName the topic name
     */
    void unsubscribe(String topicName);

    /**
     * Unsubscribe from a set of topics
     * @param topicList a set of topic names
     */
    void unsubscribe(Set<String> topicList);

    /**
     * Get all topics that the subscriber is subscribed to
     *
     * @return a list of topic subscriptions
     */
    Set<Subscription> getSubscriptions();

    /**
     * Determine whether the subscriber is subscribed to a given topic
     *
     * @param topicName the name of the topic
     * @return true if it is subscribed to the topic, false otherwise
     */
    boolean isSubscribed(String topicName);
}
