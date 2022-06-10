package worker.processor;

import messagequeue.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MessageShufflerProcessor implements MessageProcessor {
    private Logger logger = LoggerFactory.getLogger(MessageShufflerProcessor.class);

    @Override
    public void process(String message) {
        try {
            Thread.sleep(3500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        List<String> letters = Arrays.asList(message.split(""));
        Collections.shuffle(letters);
        StringBuilder shuffled = new StringBuilder();
        for (String letter : letters) {
            shuffled.append(letter);
        }

        logger.info(shuffled.toString());
    }
}
