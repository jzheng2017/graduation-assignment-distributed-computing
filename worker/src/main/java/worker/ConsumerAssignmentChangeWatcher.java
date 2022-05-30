package worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.KeyPrefix;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This watcher is responsible for listening to changes to all consumer assignments that is related to the partition that has been assigned to the {@link Worker}.
 * If a consumer has been assigned to the partition then the worker will register and start the consumer.
 * If a consumer has been unassigned from the partition then the worker will stop and unregister the consumer.
 * If the worker has been assigned a new partition then it will stop and remove all consumer it is running and register and start the new consumers that have been assigned to the new partition.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class ConsumerAssignmentChangeWatcher {
    private static final ObjectMapper mapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(ConsumerAssignmentChangeWatcher.class);
    private WatchClient watchClient;
    private Worker worker;
    private String currentKey;
    private ConsumerManager consumerManager;
    private ConsumerConfigurationWatcher consumerConfigurationWatcher;
    private boolean watcherRunning = false;

    public ConsumerAssignmentChangeWatcher(WatchClient watchClient, Worker worker, ConsumerManager consumerManager, ConsumerConfigurationWatcher consumerConfigurationWatcher) {
        this.watchClient = watchClient;
        this.worker = worker;
        this.consumerConfigurationWatcher = consumerConfigurationWatcher;
        this.currentKey = getCurrentKey();
        this.consumerManager = consumerManager;
        watchForConsumerAssignmentChange();
    }

    public void partitionChanged(int newPartitionNumber) {
        logger.info("Worker has been assigned to a new partition: {}. Watcher will be updated accordingly.", newPartitionNumber);
        watchClient.unwatch(currentKey);
        currentKey = getCurrentKey();
        watchForConsumerAssignmentChange();
    }

    private String getCurrentKey() {
        return KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();
    }

    private void watchForConsumerAssignmentChange() {
        if (worker.getAssignedPartition() >= 0) {
            watchClient.watch(currentKey, new ConsumerAssignmentChangeWatchListener());
            watcherRunning = true;
        }
    }

    @Scheduled(fixedDelay = 1000L)
    private void checkHealthWatcher() {
        if (!watcherRunning) {
            watchForConsumerAssignmentChange();
        }
    }

    private class ConsumerAssignmentChangeWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchEvent lastEvent = watchResponse.events().get(watchResponse.events().size() - 1); //we only care about the last state
            try {
                List<String> consumerList = mapper.readValue(lastEvent.currentValue(), List.class);
                Map<String, List<String>> computedConsumerState = computeAddedAndRemovedConsumers(consumerList);
                computedConsumerState.get("added").forEach(consumerId -> {
                    consumerManager.registerConsumer(consumerId);
                    consumerConfigurationWatcher.startWatchingConsumerConfiguration(consumerId);
                });
                computedConsumerState.get("removed").forEach(consumerId -> {
                    consumerManager.unregisterConsumer(consumerId);
                    consumerConfigurationWatcher.stopWatchingConsumerConfiguration(consumerId);
                });
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
            watcherRunning = false;
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
