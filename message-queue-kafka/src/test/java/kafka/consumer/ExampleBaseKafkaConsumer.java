package kafka.consumer;

import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ExampleBaseKafkaConsumer extends BaseKafkaConsumer {

    protected ExampleBaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaConsumer<String, String> kafkaConsumer, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(kafkaMessageBrokerProxy, kafkaConsumer, consumerProperties, taskManager);
    }

    @Override
    public void process(String message) {
        //not relevant
    }
}
