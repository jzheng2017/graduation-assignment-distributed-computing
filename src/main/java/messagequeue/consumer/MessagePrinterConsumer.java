package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;

/**
 * An example consumer which just prints the messages that it receives
 */
public class MessagePrinterConsumer extends BaseKafkaConsumer {
    public MessagePrinterConsumer(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(messageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
