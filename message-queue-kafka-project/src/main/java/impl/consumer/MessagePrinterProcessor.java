package impl.consumer;

import messagequeue.consumer.MessageProcessor;

/**
 * An example processor which just prints the messages that it receives
 */
public class MessagePrinterProcessor implements MessageProcessor {

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
