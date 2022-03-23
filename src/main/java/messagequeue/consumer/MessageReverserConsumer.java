package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example consumer which reversed the message and then publishes on a topic
 */
public class MessageReverserConsumer extends BaseKafkaConsumer {
    private final Logger logger = LoggerFactory.getLogger(MessageReverserConsumer.class);

    public MessageReverserConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
    }

    @Override
    public void process(String message) {
        publish("reversed", new StringBuilder(message).reverse().toString());
    }
}
