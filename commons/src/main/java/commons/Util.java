package commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class Util {
    private Logger logger = LoggerFactory.getLogger(Util.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    public String getSubstringAfterPrefix(String prefix, String original) {
        int cutOff = original.indexOf(prefix) + prefix.length();
        return original.substring(cutOff);
    }

    public <T> T toObject(String serialized, Class<T> classToMap) {
        try {
            return mapper.readValue(serialized, classToMap);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully deserialize", e);
            return null;
        }
    }

    public String serialize(Object deserialized) {
        try {
            return mapper.writeValueAsString(deserialized);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully serialize", e);
            return null;
        }
    }

    public int getTotalConcurrentTasks(WorkerStatistics workerStatistics) {
        return workerStatistics
                .concurrentTasksPerConsumer()
                .stream()
                .filter(consumer -> !consumer.internal())
                .mapToInt(ConsumerTaskCount::concurrentTaskCount)
                .sum();
    }
}
