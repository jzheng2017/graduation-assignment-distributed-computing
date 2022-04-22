package coordinator.partition;

import datastorage.KVClient;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.configuration.KeyPrefix;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PartitionWatcher {
    private Logger logger = LoggerFactory.getLogger(PartitionWatcher.class);
    private KVClient kvClient;
    private WatchClient watchClient;
    private PartitionManager partitionManager;

    public PartitionWatcher(KVClient kvClient, WatchClient watchClient, PartitionManager partitionManager) {
        this.kvClient = kvClient;
        this.watchClient = watchClient;
        this.partitionManager = partitionManager;
        watchForPartitionCountChange();
    }

    private void watchForPartitionCountChange() {
        watchClient.watch(KeyPrefix.PARTITION_COUNT, new PartitionCountChangedWatchListener());
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
            logger.error("Watching the resource/key '{}' has thrown an error.", KeyPrefix.PARTITION_COUNT, throwable);
        }

        @Override
        public void onCompleted() {
            logger.info("Stopped watching the resource/key '{}'", KeyPrefix.PARTITION_COUNT);
        }
    }
}