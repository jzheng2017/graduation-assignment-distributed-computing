package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import coordinator.dto.ConsumerProperties;
import coordinator.dto.WorkerStatistics;
import coordinator.dto.ConsumerTaskCount;
import coordinator.partition.PartitionManager;
import coordinator.worker.WorkerStatisticsDeserializer;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.KeyPrefix;
import datastorage.configuration.LockName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class ConsumerCoordinator {
    private final Logger logger = LoggerFactory.getLogger(ConsumerCoordinator.class);
    private static final int MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE = 10;
    private final Map<String, WorkerStatistics> consumerStatisticsPerInstance = new ConcurrentHashMap<>();
    private final Set<ConsumerInstanceEntry> activeConsumersOnInstances = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Queue<String> consumersToBeDistributed = new ConcurrentLinkedQueue<>();
    private final KVClient kvClient;
    private final LockClient lockClient;
    private final WorkerStatisticsDeserializer workerStatisticsDeserializer;
    private final Util util;
    private final PartitionManager partitionManager;

    public ConsumerCoordinator(KVClient kvClient, LockClient lockClient, WorkerStatisticsDeserializer workerStatisticsDeserializer, Util util, PartitionManager partitionManager) {
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.workerStatisticsDeserializer = workerStatisticsDeserializer;
        this.util = util;
        this.partitionManager = partitionManager;
    }

    public void unregisterWorker(String workerId) {
        final String registrationKey = KeyPrefix.WORKER_REGISTRATION + "-" + workerId;
        final String statisticsKey = KeyPrefix.WORKER_STATISTICS + "-" + workerId;
        String partitionAssignmentKey = null;
        Optional<Integer> partition = partitionManager.getPartitionOfWorker(workerId);
        if (partition.isPresent()) {
            partitionAssignmentKey = KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition.get();
        }
        final String workerHeartbeatKey = KeyPrefix.WORKER_HEARTBEAT + "-" + workerId;
        if (kvClient.keyExists(registrationKey)) {
            try {
                kvClient.delete(registrationKey).thenAcceptAsync(deleteResponse -> logger.info("Worker '{}' has been unregistered", workerId)).get();
                kvClient.delete(statisticsKey).thenAcceptAsync(deleteResponse -> logger.info("Consumer statistics of worker '{}' removed", workerId)).get();
                if (partitionAssignmentKey != null) {
                    kvClient.delete(partitionAssignmentKey).thenAcceptAsync(deleteResponse -> logger.info("Removed worker '{}' partition assignment", workerId)).get();
                }
                kvClient.delete(workerHeartbeatKey).thenAcceptAsync(deleteResponse -> logger.info("Removed worker '{}' heartbeat", workerId)).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Could not unregister worker '{}'", workerId, e);
            }
        } else {
            logger.warn("Can not unregister worker '{}' because it is not registered", workerId);
        }
    }

    public void addConsumerConfiguration(String consumerConfiguration) {
        ConsumerProperties consumerProperties;
        try {
            consumerProperties = new ObjectMapper().readValue(consumerConfiguration, ConsumerProperties.class);
            addConsumerConfiguration(consumerProperties);
        } catch (JsonProcessingException e) {
            logger.warn("Could not successfully parse the consumer configuration", e);
            throw new IllegalArgumentException("Could not parse consumer configuration", e);
        }
    }

    public void addConsumerConfiguration(ConsumerProperties consumerProperties) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerProperties.name();
        try {
            kvClient.get(key)
                    .thenAccept(getResponse -> {
                        try {
                            final String storedConsumerConfiguration = getResponse.keyValues().get(key);
                            final String newConsumerConfiguration = new ObjectMapper().writeValueAsString(consumerProperties);

                            if (newConsumerConfiguration.equals(storedConsumerConfiguration)) {
                                logger.warn("Consumer configuration '{}' was not added because the provided configuration is identical to what is stored", consumerProperties.name());
                                return;
                            }
                            updateConsumerStatus(consumerProperties.name(), ConsumerStatus.UNASSIGNED);

                            kvClient.put(key, newConsumerConfiguration).thenAccept(putResponse -> {
                                if (storedConsumerConfiguration == null) {
                                    logger.info("Added consumer configuration '{}'", consumerProperties);
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
    }

    public void removeConsumerConfiguration(String consumerId) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        if (kvClient.keyExists(key)) {
            kvClient.delete(key).thenAcceptAsync(deleteResponse -> {
                try {
                    kvClient.delete(KeyPrefix.CONSUMER_STATUS + "-" + consumerId).get();
                    removeConsumerAssignment(consumerId);
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Could not successfully delete status of consumer '{}'", consumerId, e);
                }
                logger.info("Consumer configuration '{}' has been removed", consumerId);
            });
        } else {
            logger.warn("Consumer configuration '{}' can not be removed as it is not present", consumerId);
        }
    }

    private void removeConsumerAssignment(String consumerId) {
        lockClient.acquireLockAndExecute(LockName.PARTITION_CONSUMER_ASSIGNMENT_LOCK, () -> {
            try {
                kvClient.getByPrefix(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT).thenAcceptAsync(getResponse -> {
                    Map<String, String> partitionConsumerAssignments = getResponse
                            .keyValues()
                            .entrySet()
                            .stream()
                            .filter(
                                    entry -> {
                                        List<String> consumerAssignments = util.toObject(entry.getValue(), List.class);
                                        return consumerAssignments.contains(consumerId);
                                    })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    partitionConsumerAssignments.forEach(
                            (key, value) -> {
                                final int partition = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-", key));
                                final List<String> consumerAssignments = util.toObject(value, List.class);
                                consumerAssignments.remove(consumerId);
                                final String serializedConsumeAssignments = util.serialize(consumerAssignments);

                                if (serializedConsumeAssignments != null) {
                                    try {
                                        kvClient.put(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition, serializedConsumeAssignments).get();
                                    } catch (InterruptedException | ExecutionException e) {
                                        logger.warn("Could not update partition consumer assignment of partition {}", partition);
                                    }
                                } else {
                                    logger.warn("Could not remove consumer from consumer assignments due to failure in serialization");
                                }
                            });
                }).get();
                kvClient.delete(KeyPrefix.CONSUMER_STATUS + "-" + consumerId).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.warn("Could not remove assignments of consumer '{}'", consumerId, e);
            }
            return null;
        });
    }

    public int computeBestPartitionForConsumer(String consumerId) {
        List<WorkerStatistics> workerStatistics = new ArrayList<>();
        try {
            workerStatistics = new ArrayList<>(kvClient.getByPrefix(KeyPrefix.WORKER_STATISTICS).get().keyValues().values().stream().map(workerStatisticsDeserializer::deserialize).toList());
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not successfully retrieve and map the worker statistics", e);
        }

        if (workerStatistics.isEmpty()) {
            logger.warn("Could not compute a partition for consumer as there are no available workers..");
            return -1;
        }

        workerStatistics.sort(
                Comparator
                        .comparing(this::getTotalConcurrentTasks)
                        .thenComparing(WorkerStatistics::totalTasksInQueue)
                        .thenComparing(WorkerStatistics::totalTasksCompleted)
        );

        return workerStatistics.get(0).partition();
    }

    public void updateConsumerStatus(String consumerId, ConsumerStatus consumerStatus) {
        final String key = KeyPrefix.CONSUMER_STATUS + "-" + consumerId;
        try {
            kvClient.put(key, consumerStatus.toString()).get();
            logger.info("Updated status of consumer '{}' to '{}'", consumerId, consumerStatus);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Could not update status of consumer '{}'", consumerId);
        }
    }

    public ConsumerStatus getConsumerStatus(String consumerId) {
        final String key = KeyPrefix.CONSUMER_STATUS + "-" + consumerId;
        try {
            final String consumerStatus = kvClient.get(key).get().keyValues().get(key);
            if (consumerStatus == null) {
                return null;
            }
            return switch (consumerStatus) {
                case "assigned", "ASSIGNED" -> ConsumerStatus.ASSIGNED;
                case "unassigned", "UNASSIGNED" -> ConsumerStatus.UNASSIGNED;
                default -> null;
            };
        } catch (ExecutionException | InterruptedException e) {
            logger.warn("Could not get status of consumer '{}'", consumerId, e);
            return null;
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
}
