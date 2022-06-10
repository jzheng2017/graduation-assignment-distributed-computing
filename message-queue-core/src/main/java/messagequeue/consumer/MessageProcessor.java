package messagequeue.consumer;

/**
 * Implementing this interfaces allows for processing a message from a topic.
 */
public interface MessageProcessor {
    /**
     * Process the message coming from a topic
     *
     * @param message the topic message
     */
    void process(String message);
}
