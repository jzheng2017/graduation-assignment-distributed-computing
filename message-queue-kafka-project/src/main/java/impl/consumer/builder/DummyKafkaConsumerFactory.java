package impl.consumer.builder;

import impl.consumer.MessageForwarderProcessor;
import impl.consumer.MessagePrinterProcessor;
import impl.consumer.MessageReverserProcessor;
import kafka.configuration.KafkaProperties;
import kafka.consumer.KafkaConsumer;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import java.util.Properties;

/**
 * Dummy {@link Consumer} factory that serves hardcoded dummy consumers. Will later be replaced by a real implementation that can construct based on consumer properties.
 */
@Import(value = {KafkaProperties.class, KafkaMessageBrokerProxy.class})
@Service
public class DummyKafkaConsumerFactory implements ConsumerFactory {
    private Logger logger = LoggerFactory.getLogger(DummyKafkaConsumerFactory.class);
    private KafkaMessageBrokerProxy kafkaMessageBrokerProxy;
    private KafkaProperties kafkaProperties;
    private TaskManager taskManager;

    public DummyKafkaConsumerFactory(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager) {
        this.kafkaMessageBrokerProxy = kafkaMessageBrokerProxy;
        this.kafkaProperties = kafkaProperties;
        this.taskManager = taskManager;
    }

    @Override
    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getValueDeserializer());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        final var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);

        logger.info("A Kafka consumer created with the following settings: name {}, group id {}, key deserializer {}, value deserializer {}, auto commit {}",
                consumerProperties.name(),
                consumerProperties.groupId(),
                kafkaProperties.getKeyDeserializer(),
                kafkaProperties.getValueDeserializer(),
                false);

        return switch (consumerProperties.name()) {
            case "forwarder" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessageForwarderProcessor(kafkaMessageBrokerProxy));
            case "reverser" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessageReverserProcessor(kafkaMessageBrokerProxy));
            case "printer" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessagePrinterProcessor());
            default -> null;
        };
    }
}
