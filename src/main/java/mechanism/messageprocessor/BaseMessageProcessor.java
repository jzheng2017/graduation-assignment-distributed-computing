package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;

public abstract class BaseMessageProcessor implements MessageProcessor {
    private final String name;
    protected MessageBrokerProxy messageBrokerProxy;

    protected BaseMessageProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = name;
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
        process(message);
    }
}
