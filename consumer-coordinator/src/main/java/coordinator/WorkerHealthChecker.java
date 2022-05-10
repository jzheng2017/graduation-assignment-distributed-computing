package coordinator;

import commons.KeyPrefix;
import commons.Util;
import coordinator.partition.PartitionManager;
import datastorage.KVClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
@Profile(value = {"dev", "kubernetes"})
public class WorkerHealthChecker {
    private static final long DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS = 10L;
    private Logger logger = LoggerFactory.getLogger(WorkerHealthChecker.class);
    private KVClient kvClient;
    private Util util;
    private PartitionManager partitionManager;
    public WorkerHealthChecker(KVClient kvClient, Util util, PartitionManager partitionManager) {
        this.kvClient = kvClient;
        this.util = util;
        this.partitionManager = partitionManager;
    }

    @Scheduled(fixedDelay = 5000L)
    public void checkHealth() {
        kvClient.getByPrefix(KeyPrefix.WORKER_HEARTBEAT).thenAcceptAsync(
                getResponse -> {
                    Map<String, String> workerHeartbeats = getResponse.keyValues();
                    for (Map.Entry<String, String> workerHeartBeat : workerHeartbeats.entrySet()) {
                        final long timeSinceLastHeartbeatInSeconds = Instant.now().getEpochSecond() - Long.parseLong(workerHeartBeat.getValue());
                        final boolean noHeartbeatSignalReceived = timeSinceLastHeartbeatInSeconds >= DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS;
                        final String workerId = util.getSubstringAfterPrefix(KeyPrefix.WORKER_HEARTBEAT + "-", workerHeartBeat.getKey());
                        if (noHeartbeatSignalReceived) {
                            logger.warn("Worker '{}' has not been responding for {} seconds. It will be unregistered.", workerId, timeSinceLastHeartbeatInSeconds);
                            unregisterWorker(workerId);
                        }
                    }
                }
        );
    }

    private void unregisterWorker(String workerId) {
        final String registrationKey = KeyPrefix.WORKER_REGISTRATION + "-" + workerId;
        final String statisticsKey = KeyPrefix.WORKER_STATISTICS + "-" + workerId;
        final String workerHeartbeatKey = KeyPrefix.WORKER_HEARTBEAT + "-" + workerId;

        String partitionAssignmentKey = null;
        Optional<Integer> partition = partitionManager.getPartitionOfWorker(workerId);
        if (partition.isPresent()) {
            partitionAssignmentKey = KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition.get();
        }
        if (kvClient.keyExists(registrationKey)) {
            try {
                kvClient.delete(registrationKey).thenAcceptAsync(deleteResponse -> logger.info("Worker '{}' has been unregistered", workerId)).get();
                kvClient.delete(statisticsKey).thenAcceptAsync(deleteResponse -> logger.info("Consumer statistics of worker '{}' removed", workerId)).get();
                if (partitionAssignmentKey != null) {
                    kvClient.delete(partitionAssignmentKey).thenAcceptAsync(deleteResponse -> logger.info("Removed worker '{}' partition assignment", workerId)).get();
                }
                kvClient.delete(workerHeartbeatKey).thenAcceptAsync(deleteResponse -> logger.info("Removed worker '{}' heartbeat", workerId)).get();
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                logger.error("Could not unregister worker '{}'", workerId, e);
            }
        } else {
            logger.warn("Can not unregister worker '{}' because it is not registered", workerId);
        }
    }
}
