package mechanism.messageprocessor;

import mechanism.messagebroker.Publisher;
import mechanism.messagebroker.Subscriber;

public interface MessageProcessor extends Subscriber, Publisher {
    void process(String message);
}
