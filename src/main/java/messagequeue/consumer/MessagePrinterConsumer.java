package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;

/**
 * An example consumer which just prints the messages that it receives
 */
public class MessagePrinterConsumer extends BaseKafkaConsumer {
    public MessagePrinterConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
