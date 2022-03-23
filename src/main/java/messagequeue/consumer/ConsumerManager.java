package messagequeue.consumer;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.messagebroker.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link ConsumerManager} it responsible for keeping track of the consumers and all other consumer-related tasks such as (un)registering consumer(s) and providing consumer information
 */
@Service
public class ConsumerManager {
    private static final int WAIT_FOR_REMOVAL_INTERVAL_IN_MS = 50;
    private Logger logger = LoggerFactory.getLogger(ConsumerManager.class);
    private Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    private Set<String> consumersScheduledForRemoval = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private ConsumerBuilder consumerBuilder;

    public ConsumerManager(ConsumerBuilder consumerBuilder) {
        this.consumerBuilder = consumerBuilder;
    }

    public void registerConsumer(String consumerConfiguration) {
        final Consumer consumer = consumerBuilder.createConsumer(consumerConfiguration);

        registerConsumer(consumer);
    }

    public void registerConsumer(Consumer consumer) { //TODO: allowed for now because ConsumerBuilder is not build yet. Eventually it should only be the case that you pass in a configuration and a consumer gets constructed from that
        final String consumerId = consumer.getIdentifier();
        logger.info("Trying to register consumer {}", consumerId);
        if (!consumers.containsKey(consumerId)) {
            consumers.put(consumerId, consumer);
            logger.info("Successfully registered consumer {}", consumerId);
            startConsumer(consumerId);
        } else {
            logger.warn("Consumer {} has already been registered.", consumerId);
        }
    }

    public void startConsumer(String consumerId) {
        logger.info("Trying to start consumer {}", consumerId);
        Consumer consumer = consumers.get(consumerId);

        if (consumer == null) {
            logger.warn("Consumer {} can not be started because is has not been registered yet.", consumerId);
            return;
        }

        if (consumer.isRunning()) {
            logger.warn("Consumer {} is already running.", consumerId);
            return;
        }

        consumer.start();
        logger.info("Consumer {} has been started", consumerId);
    }

    public void unregisterConsumer(String consumerId) {
        logger.info("Trying to unregister consumer {}", consumerId);
        if (consumers.containsKey(consumerId)) {
            if (!consumersScheduledForRemoval.contains(consumerId)) {
                Consumer toBeRemovedConsumer = consumers.get(consumerId);
                consumersScheduledForRemoval.add(consumerId);
                stopConsumer(consumerId);

                while (!toBeRemovedConsumer.isRunning()) {
                    try {
                        logger.info("Consumer {} can not be removed yet as it is still running. Waiting for {} ms...", consumerId, WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                        Thread.sleep(WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                    } catch (InterruptedException e) {
                        logger.warn("Sleeping thread interrupted", e);
                    }
                }

                logger.info("Consumer {} has been successfully stopped", consumerId);
                consumers.remove(consumerId);
                consumersScheduledForRemoval.remove(consumerId);
                logger.info("Consumer {} successfully unregistered", consumerId);
            } else {
                logger.warn("Consumer {} is already scheduled for removal", consumerId);
            }
        }
    }

    public void stopConsumer(String consumerId) {
        logger.info("Trying to stop consumer {}", consumerId);
        Consumer consumer = consumers.get(consumerId);

        if (consumer == null) {
            logger.warn("Consumer {} can not be stopped because is has not been registered yet.", consumerId);
            return;
        }

        if (!consumer.isRunning()) {
            logger.warn("Consumer {} can not be stopped because it is not running currently.", consumerId);
            return;
        }

        consumer.stop();
    }

    public void shutdown() {
        logger.info("Shutting down ConsumerManager");
        unregisterAllConsumers();
        logger.info("ConsumerManager successfully shutdown");
    }

    public List<Consumer> getAllConsumers() {
        return consumers.values().stream().toList();
    }

    public int getTotalRunningTasks() {
        return consumers.values()
                .stream()
                .mapToInt(Consumer::getNumberOfRunningTasks)
                .sum();
    }

    public int getTotalRunningTaskForConsumer(String consumerId) {
        return consumers.get(consumerId).getNumberOfRunningTasks();
    }

    private void unregisterAllConsumers() {
        consumers.values().forEach(consumer -> unregisterConsumer(consumer.getIdentifier()));
    }
}
