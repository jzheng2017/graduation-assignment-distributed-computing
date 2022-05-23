package worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.KeyPrefix;
import commons.Util;
import datastorage.KVClient;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * This watcher is responsible for listening to a new partition assignment for this worker.
 * When it has been assigned a new partition it will subsequently retrieve all consumers that has been assigned to that partition. It will then start and run these consumers.
 * When the partition assignment has been removed from the worker then it will subsequently stop and remove all consumers that the worker is running.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class PartitionAssignmentWatcher {
    private Logger logger = LoggerFactory.getLogger(PartitionAssignmentWatcher.class);
    private WatchClient watchClient;
    private KVClient kvClient;
    private ConsumerManager consumerManager;
    private Worker worker;
    private Util util;
    private ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher;
    private ConsumerConfigurationWatcher consumerConfigurationWatcher;
    private boolean watcherRunning = false;

    public PartitionAssignmentWatcher(WatchClient watchClient, KVClient kvClient, ConsumerManager consumerManager, Worker worker, Util util, ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher, ConsumerConfigurationWatcher consumerConfigurationWatcher) {
        this.watchClient = watchClient;
        this.kvClient = kvClient;
        this.consumerManager = consumerManager;
        this.worker = worker;
        this.util = util;
        this.consumerAssignmentChangeWatcher = consumerAssignmentChangeWatcher;
        this.consumerConfigurationWatcher = consumerConfigurationWatcher;
        watchForPartitionAssignments();
    }

    private void watchForPartitionAssignments() {
        watchClient.watchByPrefix(KeyPrefix.PARTITION_ASSIGNMENT, new PartitionAssignmentWatchListener());
        watcherRunning = true;
    }

    @Scheduled(fixedDelay = 1000L)
    private void checkHealthWatcher() {
        if (!watcherRunning) {
            watchForPartitionAssignments();
        }
    }

    private class PartitionAssignmentWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            Optional<WatchEvent> latestEvent = watchResponse
                    .events()
                    .stream()
                    .filter(
                            event -> {
                                int partition = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", event.currentKey()));
                                String workerId = event.currentValue();
                                final boolean isPutEvent = event.eventType() == WatchEvent.EventType.PUT && partition != worker.getAssignedPartition() && workerId.equals(worker.getIdentifier());
                                final boolean isDeleteEvent = event.eventType() == WatchEvent.EventType.DELETE && partition == worker.getAssignedPartition();
                                return isPutEvent || isDeleteEvent;
                            }
                    )
                    .findFirst();

            latestEvent.ifPresent(event -> {
                if (event.eventType() == WatchEvent.EventType.PUT) {
                    handleUpdateEvent(event);
                } else if (event.eventType() == WatchEvent.EventType.DELETE) {
                    handleDeleteEvent();
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("An error occurred while watching resource/key '{}'", KeyPrefix.PARTITION_ASSIGNMENT, throwable);
        }

        @Override
        public void onCompleted() {
            watchClient.unwatch(KeyPrefix.PARTITION_ASSIGNMENT);
            logger.info("Stopped watching resource/key '{}'", KeyPrefix.PARTITION_ASSIGNMENT);
            watcherRunning = false;
        }

        private void handleUpdateEvent(WatchEvent event) {
            final int partition = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", event.currentKey()));
            worker.setAssignedPartition(partition);
            consumerAssignmentChangeWatcher.partitionChanged(partition);
            final String key = KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();

            try {
                if (kvClient.keyExists(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition())) {
                    List<String> consumers = new ObjectMapper().readValue(
                            kvClient.get(key)
                                    .get()
                                    .keyValues()
                                    .get(key),
                            List.class);
                    consumers.forEach(consumerId -> {
                        consumerManager.registerConsumer(consumerId);
                        consumerConfigurationWatcher.startWatchingConsumerConfiguration(consumerId);
                    });
                    logger.info("Successfully registered all consumers of new partition");
                }
            } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                logger.error("Could not successfully start the consumers", e);
            }
        }

        private void handleDeleteEvent() {
            worker.setAssignedPartition(-1);
            consumerAssignmentChangeWatcher.partitionChanged(-1);
            consumerManager.unregisterAllConsumers();
        }
    }
}
