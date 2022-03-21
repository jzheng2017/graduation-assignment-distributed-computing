package mechanism.messageprocessor;

import mechanism.configuration.KafkaProperties;
import mechanism.messagebroker.MessageBrokerProxy;

public class MessagePrinterConsumer extends BaseKafkaConsumer {
    public MessagePrinterConsumer(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
