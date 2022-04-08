package messagequeue.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Service
public class JsonConsumerConfigurationParser implements ConsumerConfigurationParser {

    @Override
    public ConsumerProperties parse(String configuration) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(configuration);
        return new ConsumerProperties(
                (String) propAndValues.get("name"),
                (String) propAndValues.get("groupId"),
                new HashSet<>((List<String>) propAndValues.get("subscriptions")),
                (int)propAndValues.get("replicas"));
    }
}
