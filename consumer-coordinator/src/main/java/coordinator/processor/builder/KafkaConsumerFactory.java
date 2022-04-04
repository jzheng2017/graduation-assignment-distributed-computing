package coordinator.processor.builder;

import coordinator.ConsumerCoordinator;
import coordinator.processor.ConsumerUpdateProcessor;
import kafka.configuration.KafkaProperties;
import kafka.consumer.KafkaConsumer;
import kafka.consumer.KafkaConsumerBuilderHelper;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import org.springframework.context.annotation.Import;

@Import(value = {KafkaConsumerBuilderHelper.class, TaskManager.class, KafkaProperties.class})
public class KafkaConsumerFactory implements ConsumerFactory {
    private KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper;
    private ConsumerCoordinator consumerCoordinator;
    private TaskManager taskManager;

    public KafkaConsumerFactory(KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper, ConsumerCoordinator consumerCoordinator, TaskManager taskManager) {
        this.kafkaConsumerBuilderHelper = kafkaConsumerBuilderHelper;
        this.consumerCoordinator = consumerCoordinator;
        this.taskManager = taskManager;
    }

    @Override
    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerBuilderHelper.getKafkaConsumer(consumerProperties);
        return switch (consumerProperties.name()) {
            case "consumer-statistics-updater" -> new KafkaConsumer(consumerProperties.name(), taskManager, kafkaConsumer, new ConsumerUpdateProcessor(consumerCoordinator));
            default -> null;
        };
    }
}
