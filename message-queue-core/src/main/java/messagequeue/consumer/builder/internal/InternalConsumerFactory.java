package messagequeue.consumer.builder.internal;

import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerProperties;

public interface InternalConsumerFactory {
    Consumer createConsumer(ConsumerProperties consumerProperties);
}
