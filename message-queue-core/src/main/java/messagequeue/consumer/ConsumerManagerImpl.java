package messagequeue.consumer;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerManagerImpl implements ConsumerManager {
    private static final int WAIT_FOR_REMOVAL_INTERVAL_IN_MS = 50;
    private Logger logger = LoggerFactory.getLogger(ConsumerManagerImpl.class);
    private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    private final Set<String> consumersScheduledForRemoval = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private ConsumerBuilder consumerBuilder;
    private final TaskManager taskManager;

    public ConsumerManagerImpl(ConsumerBuilder consumerBuilder, TaskManager taskManager) {
        this.consumerBuilder = consumerBuilder;
        this.taskManager = taskManager;
    }

    @Override
    public void registerConsumer(String consumerConfiguration) {
        final Consumer consumer = consumerBuilder.createConsumer(consumerConfiguration);

        registerConsumer(consumer);
    }

    public void registerConsumer(Consumer consumer) { //TODO: allowed for now because ConsumerBuilder is not build yet. Eventually it should only be the case that you pass in a configuration and a consumer gets constructed from that
        final String consumerId = consumer.getIdentifier();
        logger.info("Trying to register consumer '{}'", consumerId);
        if (!consumers.containsKey(consumerId)) {
            consumers.put(consumerId, consumer);
            logger.info("Successfully registered consumer '{}'", consumerId);
            startConsumer(consumerId);
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

                while (!toBeRemovedConsumer.isRunning()) {
                    try {
                        logger.info("Consumer '{}' can not be removed yet as it is still running. Waiting for {} ms...", consumerId, WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                        Thread.sleep(WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                    } catch (InterruptedException e) {
                        logger.warn("Sleeping thread interrupted", e);
                    }
                }

                logger.info("Consumer '{}' has been successfully stopped", consumerId);
                consumers.remove(consumerId);
                consumersScheduledForRemoval.remove(consumerId);
                logger.info("Consumer '{}' successfully unregistered", consumerId);
            } else {
                logger.warn("Consumer '{}' is already scheduled for removal", consumerId);
            }
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
    public void shutdown() {
        logger.info("Shutting down ConsumerManager");
        unregisterAllConsumers();
        logger.info("ConsumerManager successfully shutdown");
    }

    @Override
    public List<Consumer> getAllConsumers() {
        return consumers.values().stream().toList();
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
    public long getTotalNumberOfTasksScheduled() {
        return taskManager.getTotalNumberOfTasksScheduled();
    }

    @Override
    public void unregisterAllConsumers() {
        consumers.values().forEach(consumer -> unregisterConsumer(consumer.getIdentifier()));
    }
}
