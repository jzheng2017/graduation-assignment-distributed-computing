package messagequeue.consumer.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.MessageProcessor;
import messagequeue.consumer.builder.ConsumerBuilder;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.util.Map;

public class ConsumerManagerProcessor implements MessageProcessor {
    private ConsumerManager consumerManager;
    private ConsumerBuilder consumerBuilder;

    public ConsumerManagerProcessor(ConsumerManager consumerManager, ConsumerBuilder consumerBuilder) {
        this.consumerManager = consumerManager;
        this.consumerBuilder = consumerBuilder;
    }

    @Override
    public void process(String message) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(message);
        String actionType = (String) propAndValues.get("actionType");

        switch (actionType) {
            case "add" -> {
                Map<String, Object> consumerConfiguration = (Map<String, Object>) propAndValues.get("consumer");
                try {
                    consumerManager.registerConsumer(consumerBuilder.createConsumer(new ObjectMapper().writeValueAsString(consumerConfiguration)));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException("Processing failed.", e);
                }
            }

            case "remove" -> {
                String consumerId = (String)propAndValues.get("consumerId");
                consumerManager.unregisterConsumer(consumerId);
            }

            case "shutdown" -> consumerManager.shutdown();
        }
    }
}
