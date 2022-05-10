package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.ConsumerProperties;
import commons.KeyPrefix;
import commons.LockName;
import commons.Util;
import commons.WorkerStatistics;
import coordinator.partition.PartitionManager;
import coordinator.worker.WorkerStatisticsDeserializer;
import datastorage.KVClient;
import datastorage.LockClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class ConsumerCoordinator {
    private final Logger logger = LoggerFactory.getLogger(ConsumerCoordinator.class);
    private final KVClient kvClient;
    private final LockClient lockClient;
    private final WorkerStatisticsDeserializer workerStatisticsDeserializer;
    private final Util util;

    public ConsumerCoordinator(KVClient kvClient, LockClient lockClient, WorkerStatisticsDeserializer workerStatisticsDeserializer, Util util) {
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.workerStatisticsDeserializer = workerStatisticsDeserializer;
        this.util = util;
    }

    public void addConsumerConfiguration(String consumerConfiguration) {
        ConsumerProperties consumerProperties;
        try {
            consumerProperties = new ObjectMapper().readValue(consumerConfiguration, ConsumerProperties.class);
            addConsumerConfiguration(consumerProperties);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not successfully parse the consumer configuration", e);
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

                            kvClient.put(key, newConsumerConfiguration).thenAccept(putResponse -> {
                                if (storedConsumerConfiguration == null) {
                                    updateConsumerStatus(consumerProperties.name(), ConsumerStatus.UNASSIGNED);
                                    logger.info("Added consumer configuration '{}'", consumerProperties);
                                } else {
                                    logger.info("Consumer configuration '{}' was already present. The configuration has now been updated.", consumerProperties.name());
                                }
                            }).get();
                        } catch (JsonProcessingException e) {
                            logger.warn("Consumer '{}' could not be added as something went wrong with serialization", consumerProperties.name());
                        } catch (InterruptedException | ExecutionException e) {
                            Thread.currentThread().interrupt();
                            logger.warn("Consumer '{}' could not be stored successfully", consumerProperties.name());
                        }
                    }).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not add consumer configuration '{}'", consumerProperties.name(), e);
        }
    }

    public void removeConsumerConfiguration(String consumerId) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        if (kvClient.keyExists(key)) {
            try {
                kvClient.delete(key).thenAcceptAsync(deleteResponse -> {
                    removeConsumerAssignment(consumerId);
                    logger.info("Consumer configuration '{}' has been removed", consumerId);
                }).get();
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                logger.warn("Could not properly remove consumer configuration");
            }
        } else {
            logger.warn("Consumer configuration '{}' can not be removed as it is not present", consumerId);
        }
    }

    public void removeConsumerAssignment(String consumerId) {
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
                                        Thread.currentThread().interrupt();
                                        logger.warn("Could not update partition consumer assignment of partition {}", partition);
                                    }
                                } else {
                                    logger.warn("Could not remove consumer from consumer assignments due to failure in serialization");
                                }
                            });
                }).get();
                kvClient.put(KeyPrefix.CONSUMER_STATUS + "-" + consumerId, ConsumerStatus.UNASSIGNED.toString()).get();
                logger.info("Updated status of consumer '{}' to {}", consumerId, ConsumerStatus.UNASSIGNED);
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
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
            Thread.currentThread().interrupt();
            logger.warn("Could not successfully retrieve and map the worker statistics", e);
        }

        if (workerStatistics.isEmpty()) {
            logger.warn("Could not compute a partition for consumer as there are no available workers..");
            return -1;
        }

        workerStatistics.sort(
                Comparator
                        .comparing(util::getTotalConcurrentTasks)
                        .thenComparing(WorkerStatistics::totalTasksInQueue)
                        .thenComparing(WorkerStatistics::totalTasksCompleted)
        );
        final int partition = workerStatistics.get(0).partition();
        logger.info("Best partition computed for consumer '{}', namely partition: {}", consumerId, partition);

        return partition;
    }

    public void updateConsumerStatus(String consumerId, ConsumerStatus consumerStatus) {
        final String key = KeyPrefix.CONSUMER_STATUS + "-" + consumerId;
        try {
            kvClient.put(key, consumerStatus.toString()).get();
            logger.info("Updated status of consumer '{}' to '{}'", consumerId, consumerStatus);
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
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
            Thread.currentThread().interrupt();
            logger.warn("Could not get status of consumer '{}'", consumerId, e);
            return null;
        }
    }
}
