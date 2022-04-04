package coordinator.processor;

import coordinator.ConsumerCoordinator;
import messagequeue.consumer.ConsumerStatistics;
import messagequeue.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.util.Map;

public class ConsumerUpdateProcessor implements MessageProcessor {
    private Logger logger = LoggerFactory.getLogger(ConsumerUpdateProcessor.class);
    public ConsumerCoordinator consumerCoordinator;

    public ConsumerUpdateProcessor(ConsumerCoordinator consumerCoordinator) {
        this.consumerCoordinator = consumerCoordinator;
    }

    @Override
    public void process(String message) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(message);
        ConsumerStatistics consumerStatistics = new ConsumerStatistics((String)propAndValues.get("instanceId"), (Map<String, Integer>) propAndValues.get("concurrentTasksPerConsumer"));
        logger.info(String.valueOf(consumerStatistics));
        consumerCoordinator.updateConsumerStatisticOfInstance(consumerStatistics);
    }
}
