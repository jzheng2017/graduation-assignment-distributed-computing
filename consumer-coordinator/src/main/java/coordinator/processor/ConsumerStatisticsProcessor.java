package coordinator.processor;

import coordinator.ConsumerCoordinator;
import messagequeue.consumer.ConsumerStatistics;
import messagequeue.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.util.Map;

public class ConsumerStatisticsProcessor implements MessageProcessor {
    private Logger logger = LoggerFactory.getLogger(ConsumerStatisticsProcessor.class);
    private ConsumerCoordinator consumerCoordinator;

    public ConsumerStatisticsProcessor(ConsumerCoordinator consumerCoordinator) {
        this.consumerCoordinator = consumerCoordinator;
    }

    @Override
    public void process(String message) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(message);
        ConsumerStatistics consumerStatistics = new ConsumerStatistics(
                (String) propAndValues.get("instanceId"),
                (int) propAndValues.get("totalTasksInQueue"),
                getLong(propAndValues.get("totalTasksCompleted")),
                getLong(propAndValues.get("totalTasksScheduled")),
                (Map<String, Integer>) propAndValues.get("concurrentTasksPerConsumer")
        );
        logger.info(String.valueOf(consumerStatistics));
        consumerCoordinator.updateConsumerStatisticOfInstance(consumerStatistics);
    }

    private long getLong(Object number) {
        if (number instanceof Integer) {
            return Integer.toUnsignedLong((int) number);
        } else if (number instanceof Long) {
            return (long)number;
        }

        throw new IllegalStateException("Can not be converted to long");
    }
}
