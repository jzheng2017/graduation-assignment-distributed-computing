package impl.processor;

import messagequeue.consumer.MessageProcessor;

/**
 * An example processor which just prints the messages that it receives
 */
public class MessagePrinterProcessor implements MessageProcessor {

    @Override
    public void process(String message) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(message);
    }
}
