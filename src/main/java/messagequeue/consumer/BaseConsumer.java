package messagequeue.consumer;

import messagequeue.messagebroker.Consumer;
import messagequeue.messagebroker.MessageBrokerProxy;

public abstract class BaseConsumer implements Consumer {
    protected final String name;
    protected MessageBrokerProxy messageBrokerProxy;

    protected BaseConsumer(MessageBrokerProxy messageBrokerProxy, String name) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = name;
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }
}