package messagequeue.consumer;

import messagequeue.messagebroker.Consumer;

public interface ConsumerBuilder {
    Consumer createConsumer(ConsumerProperties consumerProperties);
}
