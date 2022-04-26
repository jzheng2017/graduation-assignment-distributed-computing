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
        this.currentKey = KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();
        this.consumerManager = consumerManager;
        watchForConsumerAssignmentChange();
    }

    public void partitionChanged(int newPartitionNumber) {
        logger.info("Worker has been assigned to a new partition: {}. Watcher will be updated accordingly.", newPartitionNumber);
        watchClient.unwatch(currentKey);
        currentKey = KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();
        watchClient.watch(currentKey, new ConsumerAssignmentChangeWatchListener());
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
            logger.error("Watching key/resource '{}' has resulted in an error", currentKey, throwable);
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
