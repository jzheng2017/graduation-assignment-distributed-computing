package messagequeue.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.messagebroker.Consumer;

/**
 * A builder class that has the ability to build a {@link Consumer} based on a consumer configuration
 */
public interface ConsumerBuilder {
    /**
     * Create a consumer from a consumer configuration
     * @param consumerConfiguration the configuration detailing the properties of the consumer
     * @return a consumer
     */
    Consumer createConsumer(String consumerConfiguration);
}
