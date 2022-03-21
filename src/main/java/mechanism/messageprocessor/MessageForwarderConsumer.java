package mechanism.messageprocessor;

import mechanism.configuration.KafkaProperties;
import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageForwarderConsumer extends BaseKafkaConsumer implements Publisher {
    private Logger logger = LoggerFactory.getLogger(MessageForwarderConsumer.class);

    public MessageForwarderConsumer(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }


    @Override
    public void process(String message) {
        publish("output", message);
    }
}
