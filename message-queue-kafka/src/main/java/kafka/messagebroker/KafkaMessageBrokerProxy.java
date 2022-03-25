package kafka.messagebroker;


import kafka.configuration.KafkaProperties;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class KafkaMessageBrokerProxy implements MessageBrokerProxy {
    private final Producer<String, String> producer;

    //for testing purposes
    public KafkaMessageBrokerProxy(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Autowired
    public KafkaMessageBrokerProxy(KafkaProperties kafkaProperties) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getKeySerializer());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getValueSerializer());
        this.producer = new KafkaProducer<>(producerProperties);
    }

    @Override
    public void sendMessage(String topicName, String message) {
        producer.send(new ProducerRecord<>(topicName, message));
        producer.flush();
    }
}