package worker.processor.builder;

import commons.ConsumerProperties;
import commons.Util;
import datastorage.KVClient;
import datastorage.LockClient;
import kafka.configuration.KafkaProperties;
import kafka.consumer.KafkaConsumer;
import kafka.consumer.KafkaConsumerBuilderHelper;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.Consumer;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import worker.processor.MessagePrinterProcessor;
import worker.processor.MessageReverserProcessor;
import worker.processor.MessageUppercaseProcessor;

/**
 * Dummy {@link Consumer} factory that serves hardcoded dummy consumers. Will later be replaced by a real implementation that can construct based on consumer properties.
 */
@Service
public class DummyKafkaConsumerFactory implements ConsumerFactory {
    private Logger logger = LoggerFactory.getLogger(DummyKafkaConsumerFactory.class);
    private KafkaMessageBrokerProxy kafkaMessageBrokerProxy;
    private KafkaProperties kafkaProperties;
    private TaskManager taskManager;
    private KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper;
    private KVClient kvClient;
    private LockClient lockClient;
    private Util util;

    public DummyKafkaConsumerFactory(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager, KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper, KVClient kvClient, LockClient lockClient, Util util) {
        this.kafkaMessageBrokerProxy = kafkaMessageBrokerProxy;
        this.kafkaProperties = kafkaProperties;
        this.taskManager = taskManager;
        this.kafkaConsumerBuilderHelper = kafkaConsumerBuilderHelper;
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.util = util;
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
            case "uppercase" -> new KafkaConsumer(consumerProperties.name(), false, taskManager, consumer, new MessageUppercaseProcessor(kafkaMessageBrokerProxy), kvClient, lockClient, util);
            case "reverser" -> new KafkaConsumer(consumerProperties.name(), false, taskManager, consumer, new MessageReverserProcessor(kafkaMessageBrokerProxy), kvClient, lockClient, util);
            case "printer" -> new KafkaConsumer(consumerProperties.name(), false, taskManager, consumer, new MessagePrinterProcessor(), kvClient, lockClient, util);
            default -> null;
        };
    }
}
