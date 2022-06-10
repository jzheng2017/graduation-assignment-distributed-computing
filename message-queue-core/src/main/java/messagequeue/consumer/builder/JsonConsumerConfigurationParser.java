package messagequeue.consumer.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.ConsumerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class JsonConsumerConfigurationParser implements ConsumerConfigurationParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(JsonConsumerConfigurationParser.class);
    @Override
    public ConsumerProperties parse(String configuration) {
        try {
            return mapper.readValue(configuration, ConsumerProperties.class);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully parse the consumer configuration", e);
            return null;
        }
    }
}
