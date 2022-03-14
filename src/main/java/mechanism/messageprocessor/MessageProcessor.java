package mechanism.messageprocessor;

import mechanism.messagebroker.Publisher;
import mechanism.messagebroker.Subscriber;

public interface MessageProcessor extends Subscriber {
    void process(String message);
}
