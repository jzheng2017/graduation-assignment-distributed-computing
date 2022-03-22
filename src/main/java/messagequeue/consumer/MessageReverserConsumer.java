package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReverserConsumer extends BaseKafkaConsumer {
    private final Logger logger = LoggerFactory.getLogger(MessageReverserConsumer.class);

    public MessageReverserConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void process(String message) {
        publish("reversed", new StringBuilder(message).reverse().toString());
    }
}
