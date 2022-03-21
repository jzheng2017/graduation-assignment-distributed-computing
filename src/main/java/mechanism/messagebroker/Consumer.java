package mechanism.messagebroker;

import mechanism.messageprocessor.MessageProcessor;

public interface Consumer extends MessageProcessor, Subscriber {
    void consume();
    void acknowledgeMessage();
}
