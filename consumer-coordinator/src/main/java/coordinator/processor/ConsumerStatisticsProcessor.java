package coordinator.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import coordinator.ConsumerCoordinator;
import messagequeue.consumer.ConsumerStatistics;
import messagequeue.consumer.ConsumerTaskCount;
import messagequeue.consumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        List<Map<String, Object>> concurrentTasksPerConsumer = (List<Map<String, Object>>) propAndValues.get("concurrentTasksPerConsumer");
        ConsumerStatistics consumerStatistics = new ConsumerStatistics(
                (String) propAndValues.get("instanceId"),
                (int) propAndValues.get("totalTasksInQueue"),
                getLong(propAndValues.get("totalTasksCompleted")),
                getLong(propAndValues.get("totalTasksScheduled")),
                getConsumerTaskCount(concurrentTasksPerConsumer),
                getLong(propAndValues.get("timestamp"))
        );
        logger.info(String.valueOf(consumerStatistics));
        consumerCoordinator.updateConsumerStatisticOfInstance(consumerStatistics);
    }

    private List<ConsumerTaskCount> getConsumerTaskCount(List<Map<String, Object>> list) {
        List<ConsumerTaskCount> consumerTaskCountList = new ArrayList<>();

        for (Map<String, Object> map : list) {
            consumerTaskCountList.add(new ConsumerTaskCount((String)map.get("consumerId"), (int)map.get("count")));
        }

        return consumerTaskCountList;
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
