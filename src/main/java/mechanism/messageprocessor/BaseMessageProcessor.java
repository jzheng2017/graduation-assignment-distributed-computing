package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;

public abstract class BaseMessageProcessor implements MessageProcessor {
    protected final String name;
    protected MessageBrokerProxy messageBrokerProxy;

    protected BaseMessageProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = name;
    }

    @Override
    public void subscribe(String topicName) {
        messageBrokerProxy.subscribeToTopic(topicName, name);
        registerSubscription(topicName);
    }

    public abstract void registerSubscription(String topicName);

    @Override
    public void unsubscribe(String topicName) {
        messageBrokerProxy.unsubscribeToTopic(topicName, name);
    }
}
