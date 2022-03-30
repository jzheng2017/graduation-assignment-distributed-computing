package impl.consumer;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.MessageBrokerProxy;

/**
 * An example processor which reversed the message and then publishes on a topic
 */
public class MessageReverserProcessor implements MessageProcessor {
    private MessageBrokerProxy messageBrokerProxy;

    public MessageReverserProcessor(MessageBrokerProxy messageBrokerProxy) {
        this.messageBrokerProxy = messageBrokerProxy;
    }

    @Override
    public void process(String message) {
        messageBrokerProxy.sendMessage("reversed", new StringBuilder(message).reverse().toString());
    }
}
