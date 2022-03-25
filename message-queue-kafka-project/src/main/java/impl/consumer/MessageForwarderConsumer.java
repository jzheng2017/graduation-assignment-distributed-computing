package impl.consumer;

import kafka.configuration.KafkaProperties;
import kafka.consumer.BaseKafkaConsumer;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example consumer which basically just forwards the message by publishing to another topic
 */
public class MessageForwarderConsumer extends BaseKafkaConsumer {
    private final Logger logger = LoggerFactory.getLogger(MessageForwarderConsumer.class);

    public MessageForwarderConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
    }

    @Override
    public void process(String message) {
        publish("output", message);
    }
}
