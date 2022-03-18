package mechanism.messageprocessor;

import mechanism.configuration.KafkaProperties;
import mechanism.messagebroker.MessageBrokerProxy;
import org.springframework.stereotype.Service;

@Service
public class MessagePrinterProcessor extends KafkaMessageProcessor {
    public MessagePrinterProcessor(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
