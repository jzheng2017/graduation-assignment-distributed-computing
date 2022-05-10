package coordinator.partition;

import commons.KeyPrefix;
import commons.LockName;
import commons.Util;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.dto.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing partitions such as assigning partition to workers and computing best partition.
 */
@Service
public class PartitionManager {
    private Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private KVClient kvClient;
    private LockClient lockClient;
    private Util util;
    private Set<Integer> assignedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public PartitionManager(KVClient kvClient, LockClient lockClient, Util util) {
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.util = util;
    }

    /**
     * Assigns a partition to a worker
     *
     * @param partition the partition number (0-indexed)
     * @param workerId  the id of the worker instance
     */
    public void assignPartition(int partition, String workerId) {
        logger.info("Start partition assignment process of partition '{}' for worker '{}'", partition, workerId);
        lockClient.acquireLockAndExecute(
                LockName.PARTITION_ASSIGNMENT_LOCK,
                () -> {
                    try {
                        return kvClient.get(KeyPrefix.PARTITION_COUNT).thenAcceptAsync(
                                partitionResponse -> {
                                    int partitionCount = Integer.parseInt(partitionResponse.keyValues().get(KeyPrefix.PARTITION_COUNT));

                                    if (partition >= partitionCount) {
                                        logger.warn("Can not assign partition {} to worker {} as it exceeds the number of available partitions (0-indexed), namely: {}", partition, workerId, partitionCount);
                                        return;
                                    }

                                    if (!getAvailableWorkers().contains(workerId)) {
                                        logger.warn("Can not assign partition {} to worker {} as the worker does not exist.", partition, workerId);
                                        return;
                                    }

                                    getPartitionOfWorker(workerId).ifPresentOrElse(
                                            assignedPartition -> logger.warn("Worker '{}' has already been assigned to the partition {}", workerId, assignedPartition),
                                            () -> getWorkerAssignedToPartition(partition).ifPresentOrElse(
                                                    assignedWorker -> logger.info("Partition {} has already been assigned to worker '{}'", partition, assignedWorker),
                                                    () -> {
                                                        try {
                                                            kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition, workerId).thenAcceptAsync(putResponse -> logger.info("Partition {} has been assigned to worker '{}'", partition, workerId)).get();
                                                        } catch (InterruptedException | ExecutionException e) {
                                                            Thread.currentThread().interrupt();
                                                            logger.error("Could not assign partition '{}' to worker '{}'", partition, workerId);
                                                        }
                                                    }
                                            )
                                    );
                                }
                        ).get();
                    } catch (InterruptedException | ExecutionException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Could not successfully assign partition '{}' to worker '{}'", partition, workerId, e);
                    }
                    return null;
                }
        );

        logger.info("Partition assignment of partition '{}' for worker '{}' finished", partition, workerId);
    }

    /**
     * Remove partition assignment from a worker
     *
     * @param partition the partition number (0-indexed)
     */
    public void removePartitionAssignment(int partition) {
        logger.info("Trying to remove partition assignment of partition '{}'", partition);
        final String partitionAssignmentKey = KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition;
        lockClient.acquireLockAndExecute(
                LockName.PARTITION_ASSIGNMENT_LOCK,
                () -> {
                    if (kvClient.keyExists(partitionAssignmentKey)) {
                        try {
                            kvClient.delete(partitionAssignmentKey).thenAcceptAsync(deleteResponse -> logger.info("Partition assignment for partition {} removed", partition)).get();
                        } catch (InterruptedException | ExecutionException e) {
                            Thread.currentThread().interrupt();
                            logger.warn("Could not remove partition '{}'", partition, e);
                        }
                    } else {
                        logger.info("Partition assignment for partition {} can not be removed as there is no assignment present.", partition);
                    }

                    return null;
                }
        );
    }

    public int computeBestPartition() {
        return lockClient.acquireLockAndExecute(
                LockName.PARTITION_ASSIGNMENT_LOCK,
                () -> {
                    logger.info("Computing best partition...");
                    int numberOfPartitions = getNumberOfPartitions();
                    Map<Integer, String> partitionAssignments = getPartitionAssignments();

                    for (int i = 0; i < numberOfPartitions; i++) {
                        if (!partitionAssignments.containsKey(i)) {
                            logger.info("Computed best partition: {}", i);
                            return i; //return lowest partition
                        }
                    }

                    logger.warn("No partition could be computed.");
                    return Integer.MIN_VALUE; //means no partition was available
                }
        );
    }

    public Map<Integer, String> getPartitionAssignments() {
        try {
            return kvClient
                    .getByPrefix(KeyPrefix.PARTITION_ASSIGNMENT)
                    .get()
                    .keyValues()
                    .entrySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    entry -> Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", entry.getKey())),
                                    Map.Entry::getValue)
                    );
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not retrieve partition assignments", e);
            return new HashMap<>();
        }
    }

    /**
     * Replace the current number of partitions by the new partition count
     *
     * @param partitionCount the number of partitions
     */
    public void createPartitions(int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("The number of partitions must be greater than 0.");
        }

        try {
            kvClient.put(KeyPrefix.PARTITION_COUNT, Integer.toString(partitionCount))
                    .thenAcceptAsync(putResponse -> logger.info(
                            "Updated partition count to {}, old partition count was: {}",
                            partitionCount,
                            putResponse.prevValue().isEmpty() ? 0 : putResponse.prevValue())
                    ).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not successfully create/update partition count to '{}'", partitionCount);
        }
    }

    /**
     * Retrieve the number of available partitions
     *
     * @return number of partitions
     */
    public int getNumberOfPartitions() {
        try {
            final String partitionCount = kvClient.get(KeyPrefix.PARTITION_COUNT).get().keyValues().get(KeyPrefix.PARTITION_COUNT);
            return Integer.parseInt(partitionCount);
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not retrieve the number of partitions. Defaulting to partition count that was known on startup.", e);
            throw new IllegalStateException("Could not get the number of partitions.", e);
        }
    }

    /**
     * Get the partition number that was assigned to the worker
     *
     * @param workerId the id of the worker
     * @return the partition number
     */
    public Optional<Integer> getPartitionOfWorker(String workerId) {
        try {
            Map<String, String> partitionAssignments = kvClient.getByPrefix(KeyPrefix.PARTITION_ASSIGNMENT).get().keyValues();
            for (Map.Entry<String, String> partitionAssignment : partitionAssignments.entrySet()) {
                if (partitionAssignment.getValue().equals(workerId)) {
                    int partitionNumber = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", partitionAssignment.getKey()));

                    logger.info("Partition assignment for worker '{}' found, namely: {}", workerId, partitionNumber);
                    return Optional.of(partitionNumber);
                }
            }

            logger.info("Partition of worker '{}' could not be found. Either the worker does not exist or it has not been assigned a partition", workerId);
            return Optional.empty();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not get partition of worker '{}'", workerId, e);
            return Optional.empty();
        }
    }

    public void resetPartitionAssignmentsAndReassign() {
        lockClient.acquireLockAndExecute(
                LockName.PARTITION_ASSIGNMENT_LOCK,
                () -> {
                    try {
                        return kvClient.deleteByPrefix(KeyPrefix.PARTITION_ASSIGNMENT).thenAcceptAsync(deleteResponse -> assignPartitionsToWorkers()).get();
                    } catch (InterruptedException | ExecutionException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Something went wrong will resetting and reassigning the partition assignments..", e);
                        return null;
                    }
                }
        );
    }

    /**
     * Get the worker that has been assigned to the partition
     *
     * @param partition the partition number (0-indexed)
     * @return the id of the worker
     */
    private Optional<String> getWorkerAssignedToPartition(int partition) {
        try {
            String workerId = kvClient.get(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition).get().keyValues().get(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition);

            return Optional.ofNullable(workerId);
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not retrieve worker that is assigned to partition {}", partition, e);
            return Optional.empty();
        }
    }

    private void assignPartitionsToWorkers() {
        List<String> workers = getAvailableWorkers();
        int partitionCount = getNumberOfPartitions();
        //when all partition assignments are reset, just use a round-robin assignment
        if (workers.size() > partitionCount) {
            logger.warn("There are more workers available than partitions. Partitions: {}, Workers: {}", partitionCount, workers.size());
            for (int i = 0; i < partitionCount; i++) {
                kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + i, workers.get(i));
            }
        } else {
            logger.warn("There are more partitions available than workers. Partitions: {}, Workers: {}", partitionCount, workers.size());
            for (int i = 0; i < workers.size(); i++) {
                kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + i, workers.get(i));
            }
        }
    }

    private List<String> getAvailableWorkers() {
        try {
            return kvClient
                    .getByPrefix(KeyPrefix.WORKER_REGISTRATION)
                    .thenApply(GetResponse::keyValues)
                    .get()
                    .keySet()
                    .stream()
                    .map(key -> util.getSubstringAfterPrefix(KeyPrefix.WORKER_REGISTRATION + "-", key))
                    .toList();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not get the available workers", e);
            return new ArrayList<>();
        }
    }
}
