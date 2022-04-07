package kafka.consumer.builder;

import kafka.consumer.KafkaConsumerBuilderHelper;
import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.internal.InternalConsumerFactory;
import messagequeue.consumer.processor.ConsumerManagerProcessor;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaInternalConsumerFactory implements InternalConsumerFactory {
    private KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper;
    private ConsumerManager consumerManager;
    private ConsumerBuilder consumerBuilder;
    private TaskManager taskManager;

    public KafkaInternalConsumerFactory(KafkaConsumerBuilderHelper kafkaConsumerBuilderHelper, ConsumerManager consumerManager, ConsumerBuilder consumerBuilder, TaskManager taskManager) {
        this.kafkaConsumerBuilderHelper = kafkaConsumerBuilderHelper;
        this.consumerManager = consumerManager;
        this.consumerBuilder = consumerBuilder;
        this.taskManager = taskManager;
    }

    @Override
    public Consumer createConsumer(ConsumerProperties consumerProperties) {
        KafkaConsumer<String, String> consumer = kafkaConsumerBuilderHelper.getKafkaConsumer(consumerProperties);
        return switch (consumerProperties.name()) {
            case "consumer-manager" -> new kafka.consumer.KafkaConsumer(consumerProperties.name(), true, taskManager, consumer, new ConsumerManagerProcessor(consumerManager, consumerBuilder));
            default -> null;
        };
    }
}
