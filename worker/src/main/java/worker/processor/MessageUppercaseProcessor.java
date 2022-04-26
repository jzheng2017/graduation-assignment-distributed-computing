package worker.processor;

import messagequeue.consumer.MessageProcessor;
import messagequeue.messagebroker.MessageBrokerProxy;

/**
 * An example processor which basically just capitalizes the message and publishes to another topic
 */
public class MessageUppercaseProcessor implements MessageProcessor {
    private MessageBrokerProxy messageBrokerProxy;

    public MessageUppercaseProcessor(MessageBrokerProxy messageBrokerProxy) {
        this.messageBrokerProxy = messageBrokerProxy;
    }

    @Override
    public void process(String message) {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.messageBrokerProxy.sendMessage("output", message.toUpperCase());
    }
}
