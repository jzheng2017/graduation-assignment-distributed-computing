package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
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
