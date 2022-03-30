package kafka.consumer;

import messagequeue.consumer.BaseConsumer;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.MessageProcessor;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A Kafka implementation of the {@link messagequeue.messagebroker.Consumer} interface
 */
public class KafkaConsumer extends BaseConsumer {
    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    //constructor only for unit test purposes
    protected KafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer, ConsumerProperties consumerProperties, TaskManager taskManager, MessageProcessor messageProcessor) {
        super(consumerProperties.name(), taskManager, messageProcessor);
        this.consumer = consumer;
    }

    protected Consumer<String, String> consumer;

    public KafkaConsumer(String name, TaskManager taskManager, Consumer<String, String> consumer, MessageProcessor messageProcessor) {
        super(name, taskManager, messageProcessor);
        this.consumer = consumer;
    }

    @Override
    public List<String> poll() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        final int batchSize = records.count();

        if (batchSize > 0) {
            logger.info("Consumer '{}' found {} new message(s)", name, batchSize);
            List<String> messages = new ArrayList<>();
            records.forEach(record -> messages.add(record.value()));

            return messages;
        }

        return new ArrayList<>();
    }

    @Override
    public void cleanup() {
        consumer.close();
    }

    @Override
    public void acknowledge() {
        consumer.commitAsync();
        logger.info("Message offset committed by consumer '{}'", name);
    }

    public Consumer<String, String> getConsumer() {
        return consumer;
    }
}