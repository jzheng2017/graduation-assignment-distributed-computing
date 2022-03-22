package messagequeue.consumer;

import messagequeue.messagebroker.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerManager {
    private static final int WAIT_FOR_REMOVAL_INTERVAL_IN_MS = 50;
    private Logger logger = LoggerFactory.getLogger(ConsumerManager.class);
    private Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    private Set<String> consumersScheduledForRemoval = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private ConsumerBuilder consumerBuilder;

    public ConsumerManager(ConsumerBuilder consumerBuilder) {
        this.consumerBuilder = consumerBuilder;
    }

    public void registerConsumer(ConsumerProperties consumerProperties) {
        final Consumer consumer = consumerBuilder.createConsumer(consumerProperties);

        final String consumerId = consumer.getIdentifier();
        logger.info("Trying to register consumer with id {}", consumerId);
        if (!consumers.containsKey(consumerId)) {
            consumers.put(consumerId, consumer);
            consumer.start();
            logger.info("Successfully registered consumer with id {}", consumerId);
        } else {
            throw new IllegalArgumentException(String.format("Consumer %s has already been registered.", consumerId));
        }
    }

    public void unregisterConsumer(String consumerId) {
        logger.info("Trying to unregister consumer with id {}", consumerId);
        if (consumers.containsKey(consumerId)) {
            if (!consumersScheduledForRemoval.contains(consumerId)) {
                Consumer toBeRemovedConsumer = consumers.get(consumerId);
                consumersScheduledForRemoval.add(consumerId);
                toBeRemovedConsumer.stop();

                while (!toBeRemovedConsumer.isRunning()) {
                    try {
                        logger.info("Consumer {} can not be removed yet as it is still running. Waiting for {} ms...", consumerId, WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                        Thread.sleep(WAIT_FOR_REMOVAL_INTERVAL_IN_MS);
                    } catch (InterruptedException e) {
                        logger.warn("Sleeping thread interrupted", e);
                    }
                }

                consumers.remove(consumerId);
                consumersScheduledForRemoval.remove(consumerId);
                logger.info("Consumer with id {} successfully unregistered", consumerId);
            } else {
                throw new IllegalArgumentException(String.format("Consumer %s is already scheduled for removal", consumerId));
            }
        }
    }
}
