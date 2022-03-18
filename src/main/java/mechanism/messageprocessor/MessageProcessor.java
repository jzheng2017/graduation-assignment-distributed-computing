package mechanism.messageprocessor;

import mechanism.messagebroker.Consumer;
import mechanism.messagebroker.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MessageProcessor extends Subscriber, Consumer {
    Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    void process(String message);
}
