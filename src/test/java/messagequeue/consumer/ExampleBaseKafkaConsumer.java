package messagequeue.consumer;

import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ExampleBaseKafkaConsumer extends BaseKafkaConsumer {

    protected ExampleBaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaConsumer<String, String> kafkaConsumer) {
        super(kafkaMessageBrokerProxy, kafkaConsumer);
    }

    @Override
    public void process(String message) {
        //not relevant
    }
}
