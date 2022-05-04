package messagequeue.consumer;

import java.util.List;
import java.util.Map;

/**
 * An interface that allows for consuming messages from a topic
 */
public interface Consumer {
    /**
     * Gets the identifier of the consumer
     * @return consumer identifier
     */
    String getIdentifier();
    /**
     * Consume messages from topics
     */
    void consume();

    /**
     * Acknowledge that the consumed messages have been successfully processed
     */
    void acknowledge();

    /**
     * Set the commit offset of where the consumer should start reading from the topic
     *
     * @param topic  the topic it should commit the offset to
     * @param offset the commit offset
     */
    void commitOffset(String topic, int offset);

    /**
     * Get the commit offset of a particular topic the consumer is on
     *
     * @return the offset
     */
    int getCommitOffset(String topic);

    /**
     * Starts the consumer by setting the relevant flags
     */
    void start();

    /**
     * Stops the consumer by setting the relevant flags
     */
    void stop();

    /**
     * Polls and returns messages separated by topic
     * @return map of messages of different topics
     */
    Map<String, List<String>> poll();

    void cleanup();

    /**
     * Returns whether the consumer is still running
     * @return a flag whether the consumer is still running
     */
    boolean isRunning();

    /**
     * Returns whether the consumer is for internal use
     * @return true if internal, false otherwise
     */
    boolean isInternal();
}
