package impl.processor;

import messagequeue.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example processor which just prints the messages that it receives
 */
public class MessagePrinterProcessor implements MessageProcessor {
    private Logger logger = LoggerFactory.getLogger(MessagePrinterProcessor.class);
    @Override
    public void process(String message) {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info(message);
    }
}
