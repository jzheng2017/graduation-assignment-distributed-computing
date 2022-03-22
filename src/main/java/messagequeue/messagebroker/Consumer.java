package messagequeue.messagebroker;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.subscription.Subscriber;

/**
 * An interface that allows for consuming messages from a topic
 */
public interface Consumer extends MessageProcessor, Subscriber, Publisher {
    /**
     * Consume messages from topics
     */
    void consume();

    /**
     * Acknowledge that the consumed messages have been successfully processed
     */
    void acknowledge();
}
