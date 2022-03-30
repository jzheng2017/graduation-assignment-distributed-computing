package impl.consumer;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.MessageBrokerProxy;

/**
 * An example processor which basically just forwards the message by publishing to another topic
 */
public class MessageForwarderProcessor implements MessageProcessor {
    private MessageBrokerProxy messageBrokerProxy;

    public MessageForwarderProcessor(MessageBrokerProxy messageBrokerProxy) {
        this.messageBrokerProxy = messageBrokerProxy;
    }

    @Override
    public void process(String message) {
        this.messageBrokerProxy.sendMessage("output", message);
    }
}
