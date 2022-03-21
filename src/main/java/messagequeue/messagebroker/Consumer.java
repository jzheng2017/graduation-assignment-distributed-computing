package messagequeue.messagebroker;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.subscription.Subscriber;

public interface Consumer extends MessageProcessor, Subscriber {
    void consume();
    void acknowledgeMessage();
}
