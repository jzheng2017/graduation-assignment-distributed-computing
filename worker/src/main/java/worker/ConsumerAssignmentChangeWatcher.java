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
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This watcher is responsible for listening to changes to all consumer assignments that is related to the partition that has been assigned to the {@link Worker}
 */
@Service
public class ConsumerAssignmentChangeWatcher {
    private Logger logger = LoggerFactory.getLogger(ConsumerAssignmentChangeWatcher.class);
    private WatchClient watchClient;
    private KVClient kvClient;
    private Worker worker;
    private String currentKey;
    private ConsumerManager consumerManager;

    public ConsumerAssignmentChangeWatcher(WatchClient watchClient, KVClient kvClient, Worker worker, ConsumerManager consumerManager) {
        this.watchClient = watchClient;
        this.kvClient = kvClient;
        this.worker = worker;
        this.currentKey = getCurrentKey();
        this.consumerManager = consumerManager;
        if (worker.getAssignedPartition() >= 0) {
            watchForConsumerAssignmentChange();
        }
    }

    public void partitionChanged(int newPartitionNumber) {
        logger.info("Worker has been assigned to a new partition: {}. Watcher will be updated accordingly.", newPartitionNumber);
        watchClient.unwatch(currentKey);
        currentKey = getCurrentKey();
        watchClient.watch(currentKey, new ConsumerAssignmentChangeWatchListener());
    }

    private String getCurrentKey() {
        return KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();
    }

    private void watchForConsumerAssignmentChange() {
        watchClient.watch(currentKey, new ConsumerAssignmentChangeWatchListener());
    }

    private class ConsumerAssignmentChangeWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchEvent lastEvent = watchResponse.events().get(watchResponse.events().size() - 1); //we only care about the last state
            try {
                List<String> consumerList = new ObjectMapper().readValue(lastEvent.currentValue(), List.class);
                Map<String, List<String>> computedConsumerState = computeAddedAndRemovedConsumers(consumerList);
                computedConsumerState.get("added").forEach(consumerManager::registerConsumer);
                computedConsumerState.get("removed").forEach(consumerManager::unregisterConsumer);
            } catch (JsonProcessingException e) {
                logger.warn("Error parsing current consumer assignment state", e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("An error occurred while watching resource/key '{}'", currentKey, throwable);
        }

        @Override
        public void onCompleted() {
            logger.warn("Stopped watching for key/resource '{}'", currentKey);
        }

        private Map<String, List<String>> computeAddedAndRemovedConsumers(List<String> newConsumerList) {
            Map<String, List<String>> newConsumersState = new HashMap<>();
            List<String> currentConsumerList = consumerManager.getAllConsumers();
            List<String> addedConsumers = new ArrayList<>();
            List<String> removedConsumers = new ArrayList<>();
            newConsumersState.put("added", addedConsumers);
            newConsumersState.put("removed", removedConsumers);

            for (String consumerId : newConsumerList) {
                if (!currentConsumerList.contains(consumerId)) {
                    addedConsumers.add(consumerId);
                }
            }

            for (String consumerId : currentConsumerList) {
                if (!newConsumerList.contains(consumerId)) {
                    removedConsumers.add(consumerId);
                }
            }

            return newConsumersState;
        }
    }
}
