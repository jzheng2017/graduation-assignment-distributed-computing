package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import coordinator.dto.ConsumerProperties;
import coordinator.dto.WorkerStatistics;
import coordinator.dto.ConsumerTaskCount;
import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

@Service
public class ConsumerCoordinator {
    private final Logger logger = LoggerFactory.getLogger(ConsumerCoordinator.class);
    private static final long DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS = 10L;
    private static final int MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE = 10;
    private final Map<String, WorkerStatistics> consumerStatisticsPerInstance = new ConcurrentHashMap<>();
    private final Set<String> registeredInstanceIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConsumerInstanceEntry> activeConsumersOnInstances = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<String, ConsumerProperties> consumerConfigurations = new ConcurrentHashMap<>();
    private final Queue<String> consumersToBeDistributed = new ConcurrentLinkedQueue<>();
    private final KVClient kvClient;

    public ConsumerCoordinator( KVClient kvClient) {
        this.kvClient = kvClient;
    }

    public void unregisterWorker(String workerId) {
        final String registrationKey = KeyPrefix.WORKER_REGISTRATION + "-" + workerId;
        final String statisticsKey = KeyPrefix.WORKER_STATISTICS + "-" + workerId;
        final String partitionAssignmentKey = KeyPrefix.PARTITION_ASSIGNMENT + "-" + workerId;
        if (kvClient.keyExists(registrationKey)) {
            kvClient.delete(registrationKey).thenAcceptAsync(deleteResponse -> logger.info("Worker '{}' has been unregistered", workerId));
            kvClient.delete(statisticsKey).thenAcceptAsync(deleteResponse -> logger.info("Consumer statistics of worker '{}' removed", workerId));
            kvClient.delete(partitionAssignmentKey).thenAcceptAsync(deleteResponse -> logger.info("Removed worker '{}' partition assignment", workerId));
        } else {
            logger.warn("Can not unregister worker '{}' because it is not registered", workerId);
        }
    }

    private void requeueConsumers(String instanceId) {
        List<String> consumerIds;
        synchronized (activeConsumersOnInstances) {
            consumerIds = activeConsumersOnInstances.stream().filter(entry -> entry.instanceId().equals(instanceId)).map(ConsumerInstanceEntry::consumerId).toList();
            activeConsumersOnInstances.removeIf(entry -> entry.instanceId().equals(instanceId));
            consumersToBeDistributed.addAll(consumerIds);
        }

        logger.info("Requeuing consumers {} because instance '{}' has been unregistered", consumerIds, instanceId);
    }

    public void addConsumerConfiguration(String consumerConfiguration) {
        ConsumerProperties consumerProperties;
        try {
            consumerProperties = new ObjectMapper().readValue(consumerConfiguration, ConsumerProperties.class);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully parse the consumer configuration", e);
            throw new IllegalArgumentException("Could not parse consumer configuration", e);
        }

        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerProperties.name();
        try {
            kvClient.get(key)
                    .thenAccept(getResponse -> {
                        try {
                            kvClient.put(key, new ObjectMapper().writeValueAsString(consumerProperties)).thenAccept(putResponse -> {
                                if (getResponse.keyValues().isEmpty()) {
                                    logger.info("Added consumer configuration '{}'", consumerProperties.name());
                                } else {
                                    logger.info("Consumer configuration '{}' was already present. The configuration has now been updated.", consumerProperties.name());
                                }
                            });
                        } catch (JsonProcessingException e) {
                            logger.error("Consumer '{}' could not be added as something went wrong with serialization", consumerProperties.name());
                        }
                    }).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not add consumer configuration '{}'", consumerProperties.name(), e);
        }
        logger.info("Added consumer configuration: {}", consumerProperties);
    }

    public void removeConsumerConfiguration(String consumerId) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        if (kvClient.keyExists(key)) {
            kvClient.delete(key).thenAcceptAsync(deleteResponse -> logger.info("Consumer configuration '{}' has been removed", consumerId));
        } else {
            logger.warn("Consumer configuration '{}' can not be removed as it is not present", consumerId);
        }
    }

//    @Scheduled(fixedDelay = 5000L)
//    public void distributeConsumersThatAreQueued() {
//        final int maxConsecutiveNumberOfFailsBeforeStopping = 5;
//        int tries = 0;
//        while (!consumersToBeDistributed.isEmpty()) {
//            String consumerId = consumersToBeDistributed.poll();
//            if (consumerId == null) {
//                break;
//            }
//
//            String workerId = getLeastBusyConsumerInstanceToPlaceConsumerOn(consumerId);
//            if (workerId == null) {
//                consumersToBeDistributed.add(consumerId);
//                if (tries >= maxConsecutiveNumberOfFailsBeforeStopping) {
//                    break;
//                }
//
//                tries++;
//                continue;
//            } else {
//                tries = 0;
//            }
//
//            ConsumerProperties consumerProperties = consumerConfigurations.get(consumerId);
//            consumerDistributor.addConsumer(workerId, consumerProperties);
//            activeConsumersOnInstances.add(new ConsumerInstanceEntry(workerId, consumerId));
//        }
//    }

//    @Scheduled(fixedDelay = 5000L)
//    public void checkHealth() {
//        kvClient.get(KeyPrefix.CONSUMER_STATISTICS).thenAccept(
//                getResponse -> {
//                    Map<String, String> consumerStatistics = getResponse.keyValues();
//                    for (Map.Entry<String, String> consumerStatistic : consumerStatistics.entrySet()) {
//                        final long timeSinceLastHeartbeatInSeconds = Instant.now().getEpochSecond() - Long.parseLong(consumerStatistic.getValue());
//                        final boolean noHeartbeatSignalReceived = timeSinceLastHeartbeatInSeconds >= DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS;
//                        if (noHeartbeatSignalReceived) {
//                            logger.warn("Worker '{}' has not been responding for {} seconds. It will be unregistered.", consumerStatistic.workerId(), timeSinceLastHeartbeatInSeconds);
//                            unregisterWorker(consumerStatistic.workerId());
//                        }
//                    }
//                }
//        );
//        for (ConsumerStatistics consumerStatistic : consumerStatisticsPerInstance.values()) {
//            final long timeSinceLastHeartbeatInSeconds = Instant.now().getEpochSecond() - consumerStatistic.timestamp();
//            final boolean noHeartbeatSignalReceived = timeSinceLastHeartbeatInSeconds >= DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS;
//
//            if (noHeartbeatSignalReceived) {
//                logger.warn("Instance '{}' has not been responding for {} seconds. It will be unregistered.", consumerStatistic.workerId(), timeSinceLastHeartbeatInSeconds);
//                unregisterWorker(consumerStatistic.workerId());
//            }
//        }
//    }

//    @Scheduled(fixedDelay = 5000L)
//    public void requeueConsumersThatAreDisproportionatelyConsuming() {
//        List<ConsumerStatistics> consumerStatistics = new ArrayList<>(consumerStatisticsPerInstance.values());
//        if (consumerStatistics.size() <= 1) {
//            return;
//        }
//        logger.info("Calculating whether a consumer rebalance is needed..");
//        ConsumerStatistics busiestInstance = consumerStatistics.get(0);
//        ConsumerStatistics leastBusyInstance = consumerStatistics.get(0);
//        int busiestInstanceConcurrentTaskCount = Integer.MIN_VALUE;
//        int leastBusyInstanceConcurrentTaskCount = Integer.MAX_VALUE;
//
//        for (ConsumerStatistics consumerStatistic : consumerStatistics) {
//            int currentInstanceConcurrentTaskCount = getTotalConcurrentTasks(consumerStatistic);
//            busiestInstanceConcurrentTaskCount = getTotalConcurrentTasks(busiestInstance);
//            leastBusyInstanceConcurrentTaskCount = getTotalConcurrentTasks(leastBusyInstance);
//
//            if (currentInstanceConcurrentTaskCount > busiestInstanceConcurrentTaskCount) {
//                busiestInstance = consumerStatistic;
//            }
//
//            if (currentInstanceConcurrentTaskCount < leastBusyInstanceConcurrentTaskCount) {
//                leastBusyInstance = consumerStatistic;
//            }
//        }
//
//        final int consumptionDifferenceBetweenBusiestAndLeastBusyInstance = busiestInstanceConcurrentTaskCount - leastBusyInstanceConcurrentTaskCount;
//
//        if (consumptionDifferenceBetweenBusiestAndLeastBusyInstance >= MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE) {
//            logger.warn("A consumer rebalance will be performed due to consumption imbalance between busiest and least busy instance, namely a difference of {} tasks", consumptionDifferenceBetweenBusiestAndLeastBusyInstance);
//
//            if (busiestInstance.concurrentTasksPerConsumer().isEmpty()) {
//                return;
//            }
//
//            ConsumerTaskCount busiestConsumer = busiestInstance.concurrentTasksPerConsumer().get(0);
//
//            for (ConsumerTaskCount consumer : busiestInstance.concurrentTasksPerConsumer()) {
//                if (consumer.count() > busiestConsumer.count()) {
//                    busiestConsumer = consumer;
//                }
//            }
//
//            removeConsumerFromInstance(busiestInstance.workerId(), busiestConsumer.consumerId());
//        }
//    }

    private int getTotalConcurrentTasks(WorkerStatistics workerStatistics) {
        return workerStatistics
                .concurrentTasksPerConsumer()
                .stream()
                .filter(consumer -> !consumer.internal())
                .mapToInt(ConsumerTaskCount::count)
                .sum();
    }

    private void removeConsumerFromInstance(String instanceId, String consumerId) {
//        consumerDistributor.removeConsumer(instanceId, consumerId);
        activeConsumersOnInstances.remove(new ConsumerInstanceEntry(instanceId, consumerId));
        consumersToBeDistributed.add(consumerId);
        logger.info("Consumer '{}' removed from instance '{}' due to consumer rebalance", consumerId, instanceId);
    }

    private String getLeastBusyConsumerInstanceToPlaceConsumerOn(String consumerId) {
        List<WorkerStatistics> workerStatistics = new ArrayList<>(consumerStatisticsPerInstance.values());
        if (workerStatistics.isEmpty()) {
            return null;
        }

        workerStatistics.sort(
                Comparator
                        .comparing(this::getTotalConcurrentTasks)
                        .thenComparing(WorkerStatistics::totalTasksInQueue)
                        .thenComparing(WorkerStatistics::totalTasksCompleted)
        );


        for (WorkerStatistics instance : workerStatistics) {
            if (!activeConsumersOnInstances.contains(new ConsumerInstanceEntry(instance.workerId(), consumerId))) {
                return instance.workerId();
            }
        }

        return null;
    }
}
