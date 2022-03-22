package messagequeue.messagebroker;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.subscription.Subscriber;

public interface Consumer extends MessageProcessor, Subscriber, Publisher {
    void consume();

    void acknowledgeMessage();
}
