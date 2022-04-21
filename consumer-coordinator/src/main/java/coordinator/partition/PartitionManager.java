package coordinator.partition;

import coordinator.Util;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.KeyPrefix;
import datastorage.configuration.LockNames;
import messagequeue.configuration.EnvironmentSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
public class PartitionManager {
    private Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private KVClient kvClient;
    private LockClient lockClient;
    private Util util;

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
        lockClient.acquireLockAndExecute(
                LockNames.PARTITION_LOCK,
                () -> kvClient.get(KeyPrefix.PARTITION_COUNT).thenAccept(
                        partitionResponse -> {
                            int partitionCount = Integer.parseInt(partitionResponse.keyValues().get(KeyPrefix.PARTITION_COUNT));

                            if (partition >= partitionCount) {
                                logger.warn("Can not assign partition {} to worker {} as it exceeds the number of available partitions (0-indexed), namely: {}", partition, workerId, partitionCount);
                                return;
                            }

                            getPartitionOfWorker(workerId).ifPresentOrElse(
                                    assignedPartition -> logger.warn("Worker '{}' has already been assigned to the partition {}", workerId, assignedPartition),
                                    () -> getWorkerAssignedToPartition(partition).ifPresentOrElse(
                                            assignedWorker -> logger.info("Partition {} has already been assigned to worker '{}'", partition, assignedWorker),
                                            () -> kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition, workerId).thenAccept(putResponse -> logger.info("Partition {} has been assigned to worker '{}'", partition, workerId))
                                    )
                            );
                        }
                )
        );
    }

    /**
     * Remove partition assignment from a worker
     *
     * @param partition the partition number (0-indexed)
     */
    public void removePartitionAssignment(int partition) {
        final String partitionAssignmentKey = KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition;
        lockClient.acquireLockAndExecute(
                LockNames.PARTITION_LOCK,
                () -> kvClient.get(partitionAssignmentKey).thenAccept(partitionAssignmentResponse -> {
                            if (!partitionAssignmentResponse.keyValues().isEmpty()) {
                                kvClient.delete(partitionAssignmentKey).thenAccept(deleteResponse -> logger.info("Partition assignment for partition {} removed", partition));
                            } else {
                                logger.info("Partition assignment for partition {} can not be removed as there is no assignment present.", partition);
                            }
                        }
                )
        );
    }

    /**
     * Get the partition number that was assigned to the worker
     *
     * @param workerId the id of the worker
     * @return the partition number
     */
    public Optional<Integer> getPartitionOfWorker(String workerId) {
        try {
            Map<String, String> partitionAssignments = kvClient.get(KeyPrefix.PARTITION_ASSIGNMENT).get().keyValues();
            for (Map.Entry<String, String> partitionAssignment : partitionAssignments.entrySet()) {
                if (partitionAssignment.getValue().equals(workerId)) {
                    int partitionNumber = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", partitionAssignment.getKey()));

                    logger.info("Partition assignment for worker '{}' found, namely: {}", workerId, partitionNumber);
                    return Optional.of(partitionNumber);
                }
            }

            logger.info("Partition of worker '{}' could not be found. Either the worker does not exist or it has not been assigned a partition");
            return Optional.empty();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not get partition of worker '{}'", workerId, e);
            return Optional.empty();
        }
    }

    /**
     * Get the worker that has been assigned to the partition
     *
     * @param partition the partition number (0-indexed)
     * @return the id of the worker
     */
    public Optional<String> getWorkerAssignedToPartition(int partition) {
        try {
            String workerId = kvClient.get(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition).get().keyValues().get(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition);

            return Optional.of(workerId);
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not retrieve worker that is assigned to partition {}", partition, e);
            return Optional.empty();
        }
    }

    /**
     * Replace the current number of partitions by the new partition count
     *
     * @param partitionCount the number of partitions
     */
    public void createPartitions(int partitionCount) {
        kvClient.put(KeyPrefix.PARTITION_COUNT, Integer.toString(partitionCount)).thenAccept(putResponse -> logger.info("Updated partition count to {}, old partition count was: {}", partitionCount, putResponse.prevValue()));
    }

    /**
     * Retrieve the number of available partitions
     *
     * @return number of partitions
     */
    public int getNumberOfPartitions() {
        try {
            return Integer.parseInt(kvClient.get(KeyPrefix.PARTITION_COUNT).get().keyValues().get(KeyPrefix.PARTITION_COUNT));
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not retrieve the number of partitions. Defaulting to partition count that was known on startup.", e);
            return EnvironmentSetup.NUMBER_OF_PARTITIONS;
        }
    }
}
