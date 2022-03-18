package mechanism.messagebroker;


import mechanism.configuration.KafkaProperties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class KafkaMessageBrokerProxy extends MessageBrokerProxy {
    private Producer<String, String > producer;
    public KafkaMessageBrokerProxy(KafkaProperties kafkaProperties) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
//        producer.send(new ProducerRecord<>("test1", "pp"));
//        producer.flush();
    }

    @Override
    public void sendMessage(String topicName, String message) {
        producer.send(new ProducerRecord<>(topicName, message));
        producer.flush();
    }

    @Override
    protected String getMessageFromTopic(String topicName, String subscriberName) {
        return null;
    }
}
