package impl.consumer.builder;

import impl.consumer.MessageForwarderConsumer;
import impl.consumer.MessagePrinterConsumer;
import impl.consumer.MessageReverserConsumer;
import kafka.configuration.KafkaProperties;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

/**
 * Dummy {@link Consumer} factory that serves hardcoded dummy consumers. Will later be replaced by a real implementation that can construct based on consumer properties.
 */
@Import(value = {KafkaProperties.class, KafkaMessageBrokerProxy.class})
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
