package messagequeue.messagebroker;


import messagequeue.configuration.KafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    //for testing purposes
    public KafkaMessageBrokerProxy(SubscriptionManager subscriptionManager, Producer<String, String> producer) {
        this.subscriptionManager = subscriptionManager;
        this.producer = producer;
    }

    public KafkaMessageBrokerProxy(KafkaProperties kafkaProperties) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProperties);
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
