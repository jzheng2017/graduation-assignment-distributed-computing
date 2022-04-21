package coordinator.worker;

import coordinator.Util;
import coordinator.partition.PartitionManager;
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
public class WorkerWatcher {
    private Logger logger = LoggerFactory.getLogger(WorkerWatcher.class);
    private KVClient kvClient;
    private WatchClient watchClient;
    private Util util;
    private PartitionManager partitionManager;

    public WorkerWatcher(KVClient kvClient, WatchClient watchClient, Util util, PartitionManager partitionManager) {
        this.kvClient = kvClient;
        this.watchClient = watchClient;
        this.util = util;
        this.partitionManager = partitionManager;
        watchForWorkerChanges();
    }

    private void watchForWorkerChanges() {
        watchClient.watch(KeyPrefix.WORKER_REGISTRATION, new WorkerRegistrationChangedWatchListener());
    }

    public class WorkerRegistrationChangedWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            List<WatchEvent> events = watchResponse.events();

            for (WatchEvent event : events) {
                final String workerId = util.getSubstringAfterPrefix(KeyPrefix.WORKER_REGISTRATION, event.currentKey());
                final int partition = partitionManager.computeBestPartition();
                if (partition >= 0) {
                    partitionManager.assignPartition(partition, workerId);
                } else {
                    logger.warn("Could not assign worker '{}' a partition because no (available) partition could be computed", workerId);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.warn("Error watching key/resource '{}'", KeyPrefix.WORKER_REGISTRATION, throwable);
        }

        @Override
        public void onCompleted() {
            logger.info("Stopped watching key/resource '{}'", KeyPrefix.WORKER_REGISTRATION);
        }
    }
}
