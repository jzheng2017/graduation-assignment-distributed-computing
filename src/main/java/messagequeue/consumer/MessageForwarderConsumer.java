package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
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
