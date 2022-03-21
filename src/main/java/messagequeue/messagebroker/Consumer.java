package messagequeue.messagebroker;

import messagequeue.consumer.MessageProcessor;

public interface Consumer extends MessageProcessor, Subscriber {
    void consume();
    void acknowledgeMessage();
}
