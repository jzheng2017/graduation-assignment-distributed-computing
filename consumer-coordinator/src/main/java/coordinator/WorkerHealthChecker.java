package coordinator;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Service
public class WorkerHealthChecker {
    private static final long DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS = 10L;
    private Logger logger = LoggerFactory.getLogger(WorkerHealthChecker.class);
    private KVClient kvClient;
    private ConsumerCoordinator consumerCoordinator;
    private Util util;

    public WorkerHealthChecker(KVClient kvClient, ConsumerCoordinator consumerCoordinator, Util util) {
        this.kvClient = kvClient;
        this.consumerCoordinator = consumerCoordinator;
        this.util = util;
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
                            consumerCoordinator.unregisterWorker(workerId);
                        }
                    }
                }
        );
    }
}
