package impl.processor.builder;

import impl.processor.MessageUppercaseProcessor;
import impl.processor.MessagePrinterProcessor;
import impl.processor.MessageReverserProcessor;
import kafka.configuration.KafkaProperties;
import kafka.consumer.KafkaConsumer;
import kafka.consumer.KafkaConsumerBuilderHelper;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

/**
 * Dummy {@link Consumer} factory that serves hardcoded dummy consumers. Will later be replaced by a real implementation that can construct based on consumer properties.
 */
@Import(value = {KafkaProperties.class, KafkaMessageBrokerProxy.class, KafkaConsumerBuilderHelper.class})
@Service
public class DummyKafkaConsumerFactory implements ConsumerFactory {
    private Logger logger = LoggerFactory.getLogger(DummyKafkaConsumerFactory.class);
    private KafkaMessageBrokerProxy kafkaMessageBrokerProxy;
    private KafkaProperties kafkaProperties;
    private TaskManager taskManager;
    private KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper;

    public DummyKafkaConsumerFactory(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager, KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper) {
        this.kafkaMessageBrokerProxy = kafkaMessageBrokerProxy;
        this.kafkaProperties = kafkaProperties;
        this.taskManager = taskManager;
        this.kafkaConsumerBuilderHelper = kafkaConsumerBuilderHelper;
    }

    @Override
    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = kafkaConsumerBuilderHelper.getKafkaConsumer(consumerProperties);

        logger.info("A Kafka consumer created with the following settings: name {}, group id {}, key deserializer {}, value deserializer {}, auto commit {}",
                consumerProperties.name(),
                consumerProperties.groupId(),
                kafkaProperties.getKeyDeserializer(),
                kafkaProperties.getValueDeserializer(),
                false);

        return switch (consumerProperties.name()) {
            case "uppercase" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessageUppercaseProcessor(kafkaMessageBrokerProxy));
            case "reverser" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessageReverserProcessor(kafkaMessageBrokerProxy));
            case "printer" -> new KafkaConsumer(consumerProperties.name(), taskManager, consumer, new MessagePrinterProcessor());
            default -> null;
        };
    }
}
