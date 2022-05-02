package worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datastorage.KVClient;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.configuration.KeyPrefix;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * This watcher is responsible for listening to a new partition assignment for this worker
 */
@Service
public class PartitionAssignmentWatcher {
    private Logger logger = LoggerFactory.getLogger(PartitionAssignmentWatcher.class);
    private WatchClient watchClient;
    private KVClient kvClient;
    private ConsumerManager consumerManager;
    private Worker worker;
    private Util util;
    private ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher;
    private boolean watcherRunning = false;

    public PartitionAssignmentWatcher(WatchClient watchClient, KVClient kvClient, ConsumerManager consumerManager, Worker worker, Util util, ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher) {
        this.watchClient = watchClient;
        this.kvClient = kvClient;
        this.consumerManager = consumerManager;
        this.worker = worker;
        this.util = util;
        this.consumerAssignmentChangeWatcher = consumerAssignmentChangeWatcher;
        watchForPartitionAssignments();
    }

    private void watchForPartitionAssignments() {
        watchClient.watchByPrefix(KeyPrefix.PARTITION_ASSIGNMENT, new PartitionAssignmentWatchListener());
        watcherRunning = true;
    }

    @Scheduled(fixedDelay = 5000L)
    private void checkHealthWatcher() {
        if (!watcherRunning) {
            watchForPartitionAssignments();
        }
    }

    private class PartitionAssignmentWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            Optional<WatchEvent> latestEvent = watchResponse.events().stream().filter(event -> {
                int partition = Integer.parseInt(util.getSubstringAfterPrefix(KeyPrefix.PARTITION_ASSIGNMENT + "-", event.currentKey()));
                String workerId = event.currentValue();
                return partition != worker.getAssignedPartition() && workerId.equals(worker.getIdentifier());
            }).findFirst();

            latestEvent.ifPresent(event -> {
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
                        consumers.forEach(consumerManager::registerConsumer);
                        logger.info("Successfully registered all consumers of new partition");
                    }
                } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
                    logger.error("Could not successfully start the consumers", e);
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
    }
}
