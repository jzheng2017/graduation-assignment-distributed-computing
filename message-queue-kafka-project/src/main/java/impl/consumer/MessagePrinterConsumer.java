package impl.consumer;

import kafka.configuration.KafkaProperties;
import kafka.consumer.BaseKafkaConsumer;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.taskmanager.TaskManager;

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
