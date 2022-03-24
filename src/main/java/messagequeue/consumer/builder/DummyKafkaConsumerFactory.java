package messagequeue.consumer.builder;

import messagequeue.configuration.KafkaProperties;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.MessageForwarderConsumer;
import messagequeue.consumer.MessagePrinterConsumer;
import messagequeue.consumer.MessageReverserConsumer;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.springframework.stereotype.Service;

/**
 * Dummy {@link Consumer} factory that serves hardcoded dummy consumers. Will later be replaced by a real implementation that can construct based on consumer properties.
 */
@Service
public class DummyKafkaConsumerFactory implements ConsumerFactory {
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
        return switch (consumerProperties.name()) {
            case "forwarder" -> new MessageForwarderConsumer(kafkaMessageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
            case "reverser" -> new MessageReverserConsumer(kafkaMessageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
            case "printer" -> new MessagePrinterConsumer(kafkaMessageBrokerProxy, kafkaProperties, consumerProperties, taskManager);
            default -> null;
        };
    }
}
