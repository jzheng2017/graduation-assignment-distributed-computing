package mechanism.messagebroker;

/**
 * This interface allows a class to be a subscriber to a topic
 */
public interface Subscriber {
    /**
     * Subscribe to a topic
     * @param topicName the name of the topic
     */
    void subscribe(String topicName);

    /**
     * Unsubscribe to a topic
     * @param topicName the topic name
     */
    void unsubscribe(String topicName);
}
