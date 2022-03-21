package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.MessageBrokerProxy;

public class MessagePrinterConsumer extends BaseKafkaConsumer {
    public MessagePrinterConsumer(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
