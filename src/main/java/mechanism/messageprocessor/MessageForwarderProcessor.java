package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.Publisher;

public class MessageForwarderProcessor extends BaseMessageProcessor implements Publisher {
    public MessageForwarderProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        super(messageBrokerProxy, name);
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void process(String message) {
        publish("output", message);
    }
}
