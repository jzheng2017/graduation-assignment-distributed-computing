package kafka.consumer;

import kafka.configuration.KafkaProperties;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.BaseConsumer;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.MessageProcessingException;
import messagequeue.consumer.taskmanager.Task;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.subscription.DuplicateSubscriptionException;
import messagequeue.messagebroker.subscription.NotSubscribedException;
import messagequeue.messagebroker.subscription.Subscription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Kafka implementation of the {@link messagequeue.messagebroker.Consumer} interface
 */
public abstract class BaseKafkaConsumer extends BaseConsumer {
    private Logger logger = LoggerFactory.getLogger(BaseKafkaConsumer.class);
    protected KafkaConsumer<String, String> consumer;

    //constructor only for unit test purposes
    protected BaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaConsumer<String, String> consumer, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(kafkaMessageBrokerProxy, consumerProperties, taskManager);
        this.consumer = consumer;
    }

    protected BaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties, TaskManager taskManager) {
        super(kafkaMessageBrokerProxy, consumerProperties, taskManager);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getValueDeserializer());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.consumer = new KafkaConsumer<>(properties);
        logger.info("A Kafka consumer created with the following settings: name {}, group id {}, key deserializer {}, value deserializer {}, auto commit {}",
                consumerProperties.name(),
                consumerProperties.groupId(),
                kafkaProperties.getKeyDeserializer(),
                kafkaProperties.getValueDeserializer(),
                false);

        subscribe(consumerProperties.subscriptions());
    }

    @Override
    public void consume() {
        while (!scheduledForRemoval.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                final int batchSize = records.count();

                if (batchSize > 0) {
                    logger.info("Consumer '{}' found {} new message(s)", name, batchSize);
                    List<Task> tasksToBeExecuted = new ArrayList<>();
                    records.forEach(record -> tasksToBeExecuted.add(createTask(record.value())));
                    logger.info("Consumer '{}' created {} new task(s) and will be dispatched for execution", name, tasksToBeExecuted.size());
                    taskManager.executeTasks(tasksToBeExecuted);
                    logger.info("{} tasks successfully processed by consumer '{}'", tasksToBeExecuted.size(), name);
                    acknowledge();
                }
            } catch (MessageProcessingException ex) {
                logger.error("Processing message failed.", ex);
                throw ex;
            } catch (InterruptedException ex) {
                logger.warn("Thread was interrupted.", ex);
            }
        }
        consumer.close();
        isRunning.set(false);
        logger.info("Closed consumer '{}' and stopped running", name);
    }

    @Override
    public void subscribe(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::topicName).collect(Collectors.toSet());

        if (!subscriptions.contains(topicName)) {
            subscriptions.add(topicName);
            logger.info("Adding topic {} to the subscription list of the consumer {}", topicName, name);
            consumer.subscribe(subscriptions);
        } else {
            throw new DuplicateSubscriptionException(String.format("%s is already subscribed to %s.", name, topicName));
        }
    }

    @Override
    public void subscribe(Set<String> topicList) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.addAll(topicList); //if there are duplicate subscriptions, then it's fine as they get left out and only leaving one in. the user doesn't need to know that as it won't have any effect on the subscription process.
        consumer.subscribe(subscriptions);
        logger.info("Updated subscription list of consumer {}", name);
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public void unsubscribe(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::topicName).collect(Collectors.toSet());

        if (subscriptions.contains(topicName)) {
            subscriptions.remove(topicName);
            logger.info("Removed topic {} from the subscription list of consumer {}", topicName, name);
            consumer.subscribe(subscriptions);
        } else {
            throw new NotSubscribedException(String.format("%s is not subscribed to %s.", name, topicName));
        }
    }

    @Override
    public void unsubscribe(Set<String> topicList) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::topicName).collect(Collectors.toSet());
        subscriptions.removeAll(topicList);
        consumer.subscribe(subscriptions); //the way this function works is that regardless if you want to subscribe or unsubscribe, you use this function, and it will update the subscriptions based on the list.
        logger.info("Updated subscription list of consumer {}", name);
        logger.info("New subscription list: {}", subscriptions);
    }

    @Override
    public Set<Subscription> getSubscriptions() {
        logger.info("Retrieving subscription list of consumer {}", name);
        return consumer.subscription().stream().map(Subscription::new).collect(Collectors.toSet());
    }

    @Override
    public boolean isSubscribed(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::topicName).collect(Collectors.toSet());

        return subscriptions.contains(topicName);
    }

    @Override
    public void acknowledge() {
        consumer.commitAsync();
        logger.info("Message offset committed by consumer '{}'", name);
    }
}
