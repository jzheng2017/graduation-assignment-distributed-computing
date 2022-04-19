package coordinator.processor;

import coordinator.ConsumerCoordinator;
import messagequeue.consumer.MessageProcessor;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.util.Map;

public class InstanceRegistrationProcessor implements MessageProcessor {
    private ConsumerCoordinator consumerCoordinator;

    public InstanceRegistrationProcessor(ConsumerCoordinator consumerCoordinator) {
        this.consumerCoordinator = consumerCoordinator;
    }

    @Override
    public void process(String message) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(message);

        String registrationType = (String)propAndValues.get("registrationType");
        String instanceId = (String)propAndValues.get("instanceId");

        if (registrationType.equals("register")) {
            consumerCoordinator.registerInstance(instanceId);
        } else if (registrationType.equals("unregister")) {
            consumerCoordinator.unregisterInstance(instanceId);
        }
    }
}
