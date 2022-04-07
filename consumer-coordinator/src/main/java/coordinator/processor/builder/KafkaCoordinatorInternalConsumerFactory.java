package coordinator.processor.builder;

import coordinator.ConsumerCoordinator;
import coordinator.processor.ConsumerRegistrationProcessor;
import coordinator.processor.ConsumerStatisticsProcessor;
import kafka.consumer.KafkaConsumer;
import kafka.consumer.KafkaConsumerBuilderHelper;
import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.internal.InternalConsumerFactory;
import messagequeue.consumer.taskmanager.TaskManager;
import org.springframework.stereotype.Service;

@Service
public class KafkaCoordinatorInternalConsumerFactory implements InternalConsumerFactory {
    private KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper;
    private ConsumerCoordinator consumerCoordinator;
    private TaskManager taskManager;

    public KafkaCoordinatorInternalConsumerFactory(KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper, ConsumerCoordinator consumerCoordinator, TaskManager taskManager) {
        this.kafkaConsumerBuilderHelper = kafkaConsumerBuilderHelper;
        this.consumerCoordinator = consumerCoordinator;
        this.taskManager = taskManager;
    }

    @Override
    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerBuilderHelper.getKafkaConsumer(consumerProperties);
        return switch (consumerProperties.name()) {
            case "consumer-statistics" -> new KafkaConsumer(consumerProperties.name(), true, taskManager, kafkaConsumer, new ConsumerStatisticsProcessor(consumerCoordinator));
            case "consumer-registration" -> new KafkaConsumer(consumerProperties.name(), true, taskManager, kafkaConsumer, new ConsumerRegistrationProcessor(consumerCoordinator));
            default -> null;
        };
    }
}
