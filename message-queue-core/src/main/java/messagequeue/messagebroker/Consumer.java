package messagequeue.messagebroker;

import messagequeue.consumer.taskmanager.Task;

import java.util.List;

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
     * Starts the consumer by setting the relevant flags
     */
    void start();

    /**
     * Stops the consumer by setting the relevant flags
     */
    void stop();

    List<String> poll();

    void cleanup();

    /**
     * Returns whether the consumer is still running
     * @return a flag whether the consumer is still running
     */
    boolean isRunning();
}
