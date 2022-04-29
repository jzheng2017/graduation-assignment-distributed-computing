package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class Util {
    private Logger logger = LoggerFactory.getLogger(Util.class);

    public String getSubstringAfterPrefix(String prefix, String original) {
        int cutOff = original.indexOf(prefix) + prefix.length();
        return original.substring(cutOff);
    }

    public <T> T toObject(String serialized, Class<T> classToMap) {
        try {
            return new ObjectMapper().readValue(serialized, classToMap);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully deserialize", e);
            return null;
        }
    }

    public String serialize(Object deserialized) {
        try {
            return new ObjectMapper().writeValueAsString(deserialized);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully serialize", e);
            return null;
        }
    }
}
