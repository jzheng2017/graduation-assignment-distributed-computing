package coordinator.partition;

import datastorage.KVClient;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.configuration.KeyPrefix;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * This watcher is responsible for listening to partition count change. If the number of partitions has been changed then it has to be reset and reassigned evenly over the available workers.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class PartitionWatcher {
    private Logger logger = LoggerFactory.getLogger(PartitionWatcher.class);
    private KVClient kvClient;
    private WatchClient watchClient;
    private PartitionManager partitionManager;
    private boolean watcherRunning = false;
    public PartitionWatcher(KVClient kvClient, WatchClient watchClient, PartitionManager partitionManager) {
        this.kvClient = kvClient;
        this.watchClient = watchClient;
        this.partitionManager = partitionManager;
        watchForPartitionCountChange();
    }

    private void watchForPartitionCountChange() {
        watchClient.watch(KeyPrefix.PARTITION_COUNT, new PartitionCountChangedWatchListener());
        watcherRunning = true;
    }

    @Scheduled(fixedDelay = 5000L)
    private void checkHealthWatcher() {
        if (!watcherRunning) {
            watchForPartitionCountChange();
        }
    }

    public class PartitionCountChangedWatchListener implements WatchListener {
        @Override
        public void onNext(WatchResponse watchResponse) {
            List<WatchEvent> events = watchResponse.events();
            boolean valueReallyChanged = events.stream().anyMatch(event -> !event.prevValue().equals(event.currentValue()));
            if (valueReallyChanged) {
                partitionManager.resetPartitionAssignmentsAndReassign();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("An error occurred while watching resource/key '{}'", KeyPrefix.PARTITION_COUNT, throwable);
        }

        @Override
        public void onCompleted() {
            watchClient.unwatch(KeyPrefix.PARTITION_COUNT);
            logger.info("Stopped watching resource/key '{}'", KeyPrefix.PARTITION_COUNT);
            watcherRunning = true;
        }
    }
}
