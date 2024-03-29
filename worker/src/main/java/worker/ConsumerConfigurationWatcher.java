package worker;

import commons.KeyPrefix;
import datastorage.WatchClient;
import datastorage.WatchListener;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This watcher is responsible for watching for changes to all consumer configurations of consumers that it is responsible for.
 * That means all consumers that have been assigned to the partition the worker has been assigned to.
 * If a consumer configuration has changed then it will pass the new configuration to the consumer manager so that it can "refresh" the consumer. For instance updating the topics the consumer is subscribed to.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class ConsumerConfigurationWatcher {
    private Logger logger = LoggerFactory.getLogger(ConsumerConfigurationWatcher.class);
    private WatchClient watchClient;
    private ConsumerConfigurationParser consumerConfigurationParser;
    private ConsumerManager consumerManager;

    public ConsumerConfigurationWatcher(WatchClient watchClient, ConsumerConfigurationParser consumerConfigurationParser, ConsumerManager consumerManager) {
        this.watchClient = watchClient;
        this.consumerConfigurationParser = consumerConfigurationParser;
        this.consumerManager = consumerManager;
    }

    public void startWatchingConsumerConfiguration(String consumerId) {
        watchClient.watch(KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId, new ConsumerConfigurationChangeWatchListener());
        logger.info("Started watching consumer configuration '{}'", consumerId);
    }

    public void stopWatchingConsumerConfiguration(String consumerId) {
        watchClient.unwatch(KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId);
        logger.info("Stopped watching consumer configuration '{}'", consumerId);
    }

    private class ConsumerConfigurationChangeWatchListener implements WatchListener {

        @Override
        public void onNext(WatchResponse watchResponse) {
            Set<String> updatedConsumerConfigurations = watchResponse
                    .events()
                    .stream()
                    .filter(watchEvent -> watchEvent.eventType() == WatchEvent.EventType.PUT && !watchEvent.currentValue().equals(watchEvent.prevValue()))
                    .map(watchEvent -> consumerConfigurationParser.parse(watchEvent.currentValue()).name())
                    .filter(consumerId -> consumerManager.getAllConsumers().contains(consumerId))
                    .collect(Collectors.toSet());

            updatedConsumerConfigurations.forEach(consumerId -> consumerManager.refreshConsumer(consumerId));
        }

        @Override
        public void onError(Throwable throwable) {
            //nothing
        }

        @Override
        public void onCompleted() {
            // nothing
        }
    }
}
