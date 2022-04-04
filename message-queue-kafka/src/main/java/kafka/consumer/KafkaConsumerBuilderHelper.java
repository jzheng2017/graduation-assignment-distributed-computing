package kafka.consumer;

import kafka.configuration.KafkaProperties;
import messagequeue.consumer.ConsumerProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class KafkaConsumerBuilderHelper {
    private KafkaProperties kafkaProperties;

    public KafkaConsumerBuilderHelper(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public KafkaConsumer<String, String> getKafkaConsumer(ConsumerProperties consumerProperties) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getValueDeserializer());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }
}
