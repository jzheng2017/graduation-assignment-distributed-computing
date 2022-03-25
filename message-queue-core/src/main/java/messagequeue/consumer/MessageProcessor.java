package messagequeue.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
