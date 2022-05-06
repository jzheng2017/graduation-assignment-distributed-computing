package coordinator.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.WorkerStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkerStatisticsDeserializer {
    private Logger logger = LoggerFactory.getLogger(WorkerStatisticsDeserializer.class);

    public WorkerStatistics deserialize(String serialized) {
        try {
            return new ObjectMapper().readValue(serialized, WorkerStatistics.class);
        } catch (JsonProcessingException e) {
            logger.warn("Could not deserialize the provided string", e);
            return null;
        }
    }
}
