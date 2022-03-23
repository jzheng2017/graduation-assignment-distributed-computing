package messagequeue.messagebroker;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.subscription.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An interface that allows for consuming messages from a topic
 */
public interface Consumer extends MessageProcessor, Subscriber, Publisher {
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

    /**
     * Returns whether the consumer is still running
     * @return a flag whether the consumer is still running
     */
    boolean isRunning();

    /**
     * Get the number of tasks the consumer has running
     * @return number of tasks currently running
     */
    int getNumberOfRunningTasks();
}
