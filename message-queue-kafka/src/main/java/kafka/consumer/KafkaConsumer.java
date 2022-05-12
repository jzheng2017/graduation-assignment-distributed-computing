package kafka.consumer;

import commons.Util;
import datastorage.KVClient;
import messagequeue.consumer.BaseConsumer;
import messagequeue.consumer.MessageProcessor;
import messagequeue.consumer.TaskPackageResult;
import messagequeue.consumer.TopicOffset;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Kafka implementation of the {@link messagequeue.consumer.Consumer} interface
 */
public class KafkaConsumer extends BaseConsumer {
    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    protected Consumer<String, String> consumer;

    //constructor only for unit test purposes
    protected KafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer, String name, TaskManager taskManager, MessageProcessor messageProcessor, KVClient kvClient, Util util) {
        super(name, true, taskManager, messageProcessor, kvClient, util);
        this.consumer = consumer;
    }

    @Autowired
    public KafkaConsumer(String name, boolean isInternal, TaskManager taskManager, Consumer<String, String> consumer, MessageProcessor messageProcessor, KVClient kvClient, Util util) {
        super(name, isInternal, taskManager, messageProcessor, kvClient, util);
        this.consumer = consumer;
    }

    public KafkaConsumer(String name, boolean isInternal, TaskManager taskManager, Consumer<String, String> consumer, MessageProcessor messageProcessor, KVClient kvClient, Util util, List<TopicOffset> topicOffsets) {
        super(name, isInternal, taskManager, messageProcessor, kvClient, util, topicOffsets);
        this.consumer = consumer;
    }

    @Override
    public Map<String, List<String>> poll() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        final int batchSize = records.count();

        if (batchSize > 0) {
            logger.info("Consumer '{}' found {} new message(s)", name, batchSize);
            Map<String, List<String>> messagesPerTopic = new HashMap<>();
            records.forEach(consumerRecord -> {
                if (!messagesPerTopic.containsKey(consumerRecord.topic())) {
                    messagesPerTopic.put(consumerRecord.topic(), new ArrayList<>());
                }

                messagesPerTopic.get(consumerRecord.topic()).add(consumerRecord.value());
            });

            return messagesPerTopic;
        }

        return new HashMap<>();
    }

    @Override
    public void cleanup() {
        consumer.close();
    }

    @Override
    public void acknowledge(List<TaskPackageResult> taskPackageResults) {
        boolean allTaskPackagesSuccessfullyCompleted = taskPackageResults.stream().allMatch(TaskPackageResult::successful);
        if (allTaskPackagesSuccessfullyCompleted) {
            consumer.commitSync();
        } else {
            Map<TopicPartition, OffsetAndMetadata> offsets = taskPackageResults
                    .stream()
                    .collect(
                            Collectors
                                    .toMap(
                                            entry -> new TopicPartition(entry.topic(), 0),
                                            entry -> new OffsetAndMetadata(getTopicOffset(entry.topic() + entry.totalProcessed()))
                                    )
                    );
            consumer.commitSync(offsets);
        }
        logger.info("All task packages by consumer '{}' has finished. The topic offsets have been committed to Kafka.", name);
    }

    public Consumer<String, String> getConsumer() {
        return consumer;
    }
}
