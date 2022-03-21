package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.MessageBrokerProxy;
import messagequeue.messagebroker.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReverserConsumer extends BaseKafkaConsumer implements Publisher {
    private Logger logger = LoggerFactory.getLogger(MessageReverserConsumer.class);

    public MessageReverserConsumer(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties);
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void process(String message) {
        publish("reversed", new StringBuilder(message).reverse().toString());
    }
}
