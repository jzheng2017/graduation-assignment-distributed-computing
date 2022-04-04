package messagequeue.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.Consumer;

/**
 * A factory interface that can create a {@link Consumer} from a {@link ConsumerProperties}
 */
public interface ConsumerFactory {
    /**
     * Create a {@link Consumer} from a {@link ConsumerProperties}
     * @param consumerProperties properties of the consumer
     * @return a consumer
     */
    Consumer createConsumer(ConsumerProperties consumerProperties);
}
