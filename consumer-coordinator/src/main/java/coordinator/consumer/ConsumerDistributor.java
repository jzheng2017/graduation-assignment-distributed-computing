package coordinator.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.KeyPrefix;
import commons.LockName;
import commons.Util;
import coordinator.ConsumerCoordinator;
import coordinator.ConsumerStatus;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This class is responsible for watching new consumers and assign them to a partition if they haven't been assigned already.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class ConsumerDistributor {
    private Logger logger = LoggerFactory.getLogger(ConsumerDistributor.class);
    private WatchClient watchClient;
    private ConsumerCoordinator consumerCoordinator;
    private Util util;
    private LockClient lockClient;
    private KVClient kvClient;
    private boolean watcherRunning = false;
    public ConsumerDistributor(WatchClient watchClient, ConsumerCoordinator consumerCoordinator, Util util, LockClient lockClient, KVClient kvClient) {
        this.watchClient = watchClient;
        this.consumerCoordinator = consumerCoordinator;
        this.util = util;
        this.lockClient = lockClient;
        this.kvClient = kvClient;
        watchForConsumerStatusChanges();
    }

    private void watchForConsumerStatusChanges() {
        watchClient.watchByPrefix(KeyPrefix.CONSUMER_STATUS, new ConsumerStatusWatchListener());
        watcherRunning = true;
    }

    @Scheduled(fixedDelay = 1000L)
    private void checkHealthWatcher() {
        if (!watcherRunning) {
            watchForConsumerStatusChanges();
        }
    }

    private class ConsumerStatusWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            List<WatchEvent> events = watchResponse
                    .events()
                    .stream()
                    .filter(
                            event -> event.eventType() == WatchEvent.EventType.PUT &&
                                    event.currentValue().equalsIgnoreCase(ConsumerStatus.UNASSIGNED.toString())
                    )
                    .toList();
            for (WatchEvent event : events) {
                final String consumerId = util.getSubstringAfterPrefix(KeyPrefix.CONSUMER_STATUS + "-", event.currentKey());
                int partition = consumerCoordinator.computeBestPartitionForConsumer(consumerId);
                if (partition < 0) {
                    continue;
                }
                assignConsumerToPartition(partition, consumerId);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("An error occurred while watching resource/key '{}'", KeyPrefix.CONSUMER_STATUS, throwable);
        }

        @Override
        public void onCompleted() {
            watchClient.unwatch(KeyPrefix.CONSUMER_STATUS);
            logger.info("Stopped watching resource/key '{}'", KeyPrefix.CONSUMER_STATUS);
            watcherRunning = false;
        }


        private void assignConsumerToPartition(final int partition, final String consumerId) {
            lockClient.acquireLockAndExecute(
                    LockName.PARTITION_CONSUMER_ASSIGNMENT_LOCK,
                    () -> {
                        final String partitionConsumerAssignmentKey = KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition;

                        try {
                            List<String> consumers = new ArrayList<>();

                            if (kvClient.keyExists(partitionConsumerAssignmentKey)) {
                                consumers = new ObjectMapper().readValue(kvClient.get(partitionConsumerAssignmentKey).get().keyValues().get(partitionConsumerAssignmentKey), List.class);
                            }

                            if (!consumers.contains(consumerId)) {
                                consumers.add(consumerId);
                                kvClient.put(partitionConsumerAssignmentKey, new ObjectMapper().writeValueAsString(consumers))
                                        .thenAcceptAsync(ignore -> consumerCoordinator.updateConsumerStatus(consumerId, ConsumerStatus.ASSIGNED)).get();
                                logger.info("Assigned consumer '{}' to partition '{}'", consumerId, partition);
                            } else {
                                logger.warn("Consumer '{}' has already been assigned to partition '{}'", consumerId, partition);
                            }
                        } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
                            Thread.currentThread().interrupt();
                            logger.warn("Could not successfully assign consumer '{}' to partition '{}'", consumerId, partition, e);
                        }

                        return null;
                    }
            );
        }
    }
}
