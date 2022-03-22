package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageForwarderConsumer extends BaseKafkaConsumer {
    private final Logger logger = LoggerFactory.getLogger(MessageForwarderConsumer.class);

    public MessageForwarderConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }


    @Override
    public void process(String message) {
        publish("output", message);
    }
}
