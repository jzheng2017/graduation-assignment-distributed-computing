package messagequeue.consumer;

import commons.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import messagequeue.consumer.builder.ConsumerConfigurationStore;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerManagerImpl implements ConsumerManager {
    private static final String identifier = UUID.randomUUID().toString();
    private static final int WAIT_FOR_REMOVAL_INTERVAL_IN_MS = 1000;
    private Logger logger = LoggerFactory.getLogger(ConsumerManagerImpl.class);
    private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    private final Set<String> consumersScheduledForRemoval = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final TaskManager taskManager;
    private ConsumerConfigurationStore consumerConfigurationStore;
    private ConsumerBuilder consumerBuilder;
    private ConsumerConfigurationParser consumerConfigurationParser;
    private SubscriptionManager subscriptionManager;

    //for unit test purposes only
    protected ConsumerManagerImpl(Logger logger, TaskManager taskManager, ConsumerConfigurationStore consumerConfigurationStore, ConsumerBuilder consumerBuilder, ConsumerConfigurationParser consumerConfigurationParser, SubscriptionManager subscriptionManager) {
        this(taskManager, consumerConfigurationStore, consumerBuilder, consumerConfigurationParser, subscriptionManager);
        this.logger = logger;
    }

    @Autowired
    public ConsumerManagerImpl(TaskManager taskManager, ConsumerConfigurationStore consumerConfigurationStore, ConsumerBuilder consumerBuilder, ConsumerConfigurationParser consumerConfigurationParser, SubscriptionManager subscriptionManager) {
        this.taskManager = taskManager;
        this.consumerConfigurationStore = consumerConfigurationStore;
        this.consumerBuilder = consumerBuilder;
        this.consumerConfigurationParser = consumerConfigurationParser;
        this.subscriptionManager = subscriptionManager;
    }

    public void registerConsumer(String consumerId) {
        String consumerConfiguration = consumerConfigurationStore.getConsumerConfiguration(consumerId);
        Consumer consumer = consumerBuilder.createConsumer(consumerConfiguration);
        registerConsumer(consumer);
    }

    private void registerConsumer(Consumer consumer) {
        final String consumerId = consumer.getIdentifier();
        logger.info("Trying to register consumer '{}'", consumerId);
        if (!consumers.containsKey(consumerId)) {
            consumers.put(consumerId, consumer);
            startConsumer(consumerId);
            logger.info("Successfully registered consumer '{}'", consumerId);
        } else {
            logger.warn("Consumer '{}' has already been registered.", consumerId);
        }
    }

    @Override
    public void startConsumer(String consumerId) {
        logger.info("Trying to start consumer '{}'", consumerId);
        Consumer consumer = consumers.get(consumerId);

        if (consumer == null) {
            logger.warn("Consumer '{}' can not be started because is has not been registered yet.", consumerId);
            return;
        }

        if (consumer.isRunning()) {
            logger.warn("Consumer '{}' is already running.", consumerId);
            return;
        }

        consumer.start();
        logger.info("Consumer '{}' has been started", consumerId);
    }

    @Override
    public void unregisterConsumer(String consumerId) {
        logger.info("Trying to unregister consumer '{}'", consumerId);
        if (consumers.containsKey(consumerId)) {
            if (!consumersScheduledForRemoval.contains(consumerId)) {
                Consumer toBeRemovedConsumer = consumers.get(consumerId);
                consumersScheduledForRemoval.add(consumerId);
                stopConsumer(consumerId);

                while (toBeRemovedConsumer.isRunning()) {
                    try {
                        logger.info("Consumer '{}' can not be removed yet as it is still running. Waiting for {} ms...", consumerId, WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                        Thread.sleep(WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                    } catch (InterruptedException e) {
                        logger.warn("Sleeping thread interrupted", e);
                        Thread.currentThread().interrupt();
                    }
                }

                logger.info("Consumer '{}' has been successfully stopped", consumerId);
                consumers.remove(consumerId);
                consumersScheduledForRemoval.remove(consumerId);
                logger.info("Consumer '{}' successfully unregistered", consumerId);
            } else {
                logger.warn("Consumer '{}' is already scheduled for removal", consumerId);
            }
        } else {
            logger.warn("Consumer '{}' can not be unregistered because it has not been registered", consumerId);
        }
    }

    @Override
    public void stopConsumer(String consumerId) {
        logger.info("Trying to stop consumer '{}'", consumerId);
        Consumer consumer = consumers.get(consumerId);

        if (consumer == null) {
            logger.warn("Consumer '{}' can not be stopped because is has not been registered yet.", consumerId);
            return;
        }

        if (!consumer.isRunning()) {
            logger.warn("Consumer '{}' can not be stopped because it is not running currently.", consumerId);
            return;
        }

        consumer.stop();
    }

    @Override
    public void refreshConsumer(String consumerId) {
        Consumer consumer = consumers.get(consumerId);
        if (consumer == null) {
            logger.warn("Consumer '{}' can not be updated because it can not be found", consumerId);
            return;
        }

        if (consumer.isRunning()) {
            final String consumerConfiguration = consumerConfigurationStore.getConsumerConfiguration(consumerId);
            ConsumerProperties consumerProperties = consumerConfigurationParser.parse(consumerConfiguration);
            subscriptionManager.subscribe(consumerProperties.subscriptions(), Map.of("consumer", consumer));
            logger.info("Consumer '{}' updated", consumerId);
        } else {
            logger.warn("Consumer '{}' can not be updated because it is not running anymore", consumerId);
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down ConsumerManager");
        unregisterAllConsumers();
        logger.info("ConsumerManager successfully shutdown");
    }

    @Override
    public List<String> getAllConsumers() {
        return consumers.keySet().stream().toList();
    }

    @Override
    public boolean isConsumerInternal(String consumerIdentifier) {
        synchronized (consumers) {
            if (consumers.containsKey(consumerIdentifier)) {
                return consumers.get(consumerIdentifier).isInternal();
            }
        }
        throw new IllegalArgumentException(String.format("Consumer %s is not registered", consumerIdentifier));
    }

    @Override
    public int getTotalRunningTasks() {
        return taskManager.getTotalNumberOfTasksCurrentlyExecuting();
    }

    @Override
    public int getTotalRunningTasksForConsumer(String consumerId) {
        return taskManager.getTotalNumberOfConcurrentTasksForConsumer(consumerId);
    }

    @Override
    public int getTotalNumberOfTasksInQueue() {
        return taskManager.getTotalNumberOfTasksInQueue();
    }

    @Override
    public long getTotalNumberOfCompletedTasks() {
        return taskManager.getTotalNumberOfCompletedTasks();
    }

    @Override
    public Map<String, Integer> getTotalRunningTasksForAllConsumers() {
        return taskManager.getTotalNumberOfConcurrentTasksForAllConsumers();
    }

    @Override
    public void unregisterAllConsumers() {
        consumers.values().forEach(consumer -> unregisterConsumer(consumer.getIdentifier()));
    }
}
