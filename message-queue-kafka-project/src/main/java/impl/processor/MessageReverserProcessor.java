package impl.processor;

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
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        messageBrokerProxy.sendMessage("reversed", new StringBuilder(message).reverse().toString());
    }
}
