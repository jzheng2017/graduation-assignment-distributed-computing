package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;

public abstract class BaseMessageProcessor implements MessageProcessor {
    private final String name;
    private MessageBrokerProxy messageBrokerProxy;

    protected BaseMessageProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = name;
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void subscribe(String topicName) {
        messageBrokerProxy.subscribeToTopic(topicName, name);
    }

    @Override
    public void unsubscribe(String topicName) {
        messageBrokerProxy.unsubscribeToTopic(topicName, name);
    }

    @Override
    public void poll(String topicName) {
        String message = messageBrokerProxy.receiveMessage(topicName, name);
    }
}
