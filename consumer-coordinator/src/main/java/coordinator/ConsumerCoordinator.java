package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datastorage.KVClient;
import datastorage.configuration.EtcdKeyPrefix;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.ConsumerStatistics;
import messagequeue.consumer.ConsumerTaskCount;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Service
public class ConsumerCoordinator {
    private final Logger logger = LoggerFactory.getLogger(ConsumerCoordinator.class);
    private static final long DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS = 10L;
    private static final int MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE = 10;
    private final Map<String, ConsumerStatistics> consumerStatisticsPerInstance = new ConcurrentHashMap<>();
    private final Set<String> registeredInstanceIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConsumerInstanceEntry> activeConsumersOnInstances = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<String, ConsumerProperties> consumerConfigurations = new ConcurrentHashMap<>();
    private final Queue<String> consumersToBeDistributed = new ConcurrentLinkedQueue<>();
    private final ConsumerDistributor consumerDistributor;
    private final MessageBrokerProxy messageBrokerProxy;
    private final KVClient kvClient;

    public ConsumerCoordinator(ConsumerDistributor consumerDistributor, MessageBrokerProxy messageBrokerProxy, KVClient kvClient) {
        this.consumerDistributor = consumerDistributor;
        this.messageBrokerProxy = messageBrokerProxy;
        this.kvClient = kvClient;
    }

    public void updateConsumerStatisticOfInstance(ConsumerStatistics consumerStatistics) {
        consumerStatisticsPerInstance.put(consumerStatistics.instanceId(), consumerStatistics);
    }

    public void registerWorker(String workerId) {
        final String key = EtcdKeyPrefix.WORKER_REGISTRATION + "-" + workerId;
        kvClient.get(key).thenAccept(getResponse -> {
            if (!getResponse.keyValues().isEmpty()) {
                kvClient.put(key, Long.toString(Instant.now().getEpochSecond())).thenAccept(putResponse -> logger.info("Worker '{}' has been registered", workerId));
            } else {
                logger.warn("Can not register worker '{}' because it already has been registered", workerId);
            }
        });
    }

    public void unregisterWorker(String workerId) {
        final String key = EtcdKeyPrefix.WORKER_REGISTRATION + "-" + workerId;
        kvClient.get(key).thenAccept(getResponse -> {
            if (!getResponse.keyValues().isEmpty()) {
                kvClient.delete(key).thenAccept(deleteResponse -> logger.info("Worker '{}' has been unregistered", workerId));
            } else {
                logger.warn("Can not unregister worker '{}' because it is not registered", workerId);
            }
        });
    }

    private void requeueConsumers(String instanceId) {
        List<String> consumerIds;
        synchronized (activeConsumersOnInstances) {
            consumerIds = activeConsumersOnInstances.stream().filter(entry -> entry.instanceId().equals(instanceId)).map(ConsumerInstanceEntry::consumerId).toList();
            activeConsumersOnInstances.removeIf(entry -> entry.instanceId().equals(instanceId));
            consumersToBeDistributed.addAll(consumerIds);
        }

        logger.info("Requeuing consumers {} because instance '{}' has been unregistered", consumerIds, instanceId);
    }

    public void addConsumerConfiguration(String consumerConfiguration) {
        JsonParser jsonParser = JsonParserFactory.getJsonParser();
        Map<String, Object> propAndValues = jsonParser.parseMap(consumerConfiguration);
        ConsumerProperties consumerProperties = new ConsumerProperties(
                (String) propAndValues.get("name"),
                (String) propAndValues.get("groupId"),
                new HashSet<>((List<String>) propAndValues.get("subscriptions"))
        );

        final String key = EtcdKeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerProperties.name();
        kvClient.get(key)
                .thenAccept(getResponse -> {
                    try {
                        kvClient.put(key, new ObjectMapper().writeValueAsString(consumerProperties)).thenAccept(putResponse -> {
                            if (getResponse.keyValues().isEmpty()) {
                                logger.info("Added consumer configuration '{}'", consumerProperties.name());
                            } else {
                                logger.info("Consumer configuration '{}' was already present. The configuration has now been updated.", consumerProperties.name());
                            }
                        });
                    } catch (JsonProcessingException e) {
                        logger.error("Consumer '{}' could not be added as something went wrong with serialization", consumerProperties.name());
                    }
                });
        logger.info("Added consumer configuration: {}", consumerProperties);
    }

    public void removeConsumerConfiguration(String consumerId) {
        final String key = EtcdKeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        kvClient.get(key).thenAccept(getResponse -> {
            if (!getResponse.keyValues().isEmpty()) {
                logger.warn("Consumer configuration '{}' can not be removed as it is not present", consumerId);
            } else {
                kvClient.delete(key).thenAccept(deleteResponse -> logger.info("Consumer configuration '{}' has been removed", consumerId));
            }
        });
    }

    private Set<String> getAllConsumersOfRunningOnInstance(String instanceId) {
        return activeConsumersOnInstances
                .stream()
                .filter(entry -> entry.instanceId().equals(instanceId))
                .map(ConsumerInstanceEntry::consumerId)
                .collect(Collectors.toSet());
    }

    @Scheduled(fixedDelay = 5000L)
    public void distributeConsumersThatAreQueued() {
        final int maxConsecutiveNumberOfFailsBeforeStopping = 5;
        int tries = 0;
        while (!consumersToBeDistributed.isEmpty()) {
            String consumerId = consumersToBeDistributed.poll();
            if (consumerId == null) {
                break;
            }

            String instanceId = getLeastBusyConsumerInstanceToPlaceConsumerOn(consumerId);
            if (instanceId == null) {
                consumersToBeDistributed.add(consumerId);
                if (tries >= maxConsecutiveNumberOfFailsBeforeStopping) {
                    break;
                }

                tries++;
                continue;
            } else {
                tries = 0;
            }

            ConsumerProperties consumerProperties = consumerConfigurations.get(consumerId);
            consumerDistributor.addConsumer(instanceId, consumerProperties);
            activeConsumersOnInstances.add(new ConsumerInstanceEntry(instanceId, consumerId));
        }
    }

    @Scheduled(fixedDelay = 5000L)
    public void checkHealth() {
        for (ConsumerStatistics consumerStatistic : consumerStatisticsPerInstance.values()) {
            final long timeSinceLastHeartbeatInSeconds = Instant.now().getEpochSecond() - consumerStatistic.timestamp();
            final boolean noHeartbeatSignalReceived = timeSinceLastHeartbeatInSeconds >= DEFAULT_MISSED_HEARTBEAT_FOR_REMOVAL_IN_SECONDS;

            if (noHeartbeatSignalReceived) {
                logger.warn("Instance '{}' has not been responding for {} seconds. It will be unregistered.", consumerStatistic.instanceId(), timeSinceLastHeartbeatInSeconds);
                unregisterWorker(consumerStatistic.instanceId());
            }
        }
    }

    @Scheduled(fixedDelay = 5000L)
    public void requeueConsumersThatAreDisproportionatelyConsuming() {
        List<ConsumerStatistics> consumerStatistics = new ArrayList<>(consumerStatisticsPerInstance.values());
        if (consumerStatistics.size() <= 1) {
            return;
        }
        logger.info("Calculating whether a consumer rebalance is needed..");
        ConsumerStatistics busiestInstance = consumerStatistics.get(0);
        ConsumerStatistics leastBusyInstance = consumerStatistics.get(0);
        int busiestInstanceConcurrentTaskCount = Integer.MIN_VALUE;
        int leastBusyInstanceConcurrentTaskCount = Integer.MAX_VALUE;

        for (ConsumerStatistics consumerStatistic : consumerStatistics) {
            int currentInstanceConcurrentTaskCount = getTotalConcurrentTasks(consumerStatistic);
            busiestInstanceConcurrentTaskCount = getTotalConcurrentTasks(busiestInstance);
            leastBusyInstanceConcurrentTaskCount = getTotalConcurrentTasks(leastBusyInstance);

            if (currentInstanceConcurrentTaskCount > busiestInstanceConcurrentTaskCount) {
                busiestInstance = consumerStatistic;
            }

            if (currentInstanceConcurrentTaskCount < leastBusyInstanceConcurrentTaskCount) {
                leastBusyInstance = consumerStatistic;
            }
        }

        final int consumptionDifferenceBetweenBusiestAndLeastBusyInstance = busiestInstanceConcurrentTaskCount - leastBusyInstanceConcurrentTaskCount;

        if (consumptionDifferenceBetweenBusiestAndLeastBusyInstance >= MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE) {
            logger.warn("A consumer rebalance will be performed due to consumption imbalance between busiest and least busy instance, namely a difference of {} tasks", consumptionDifferenceBetweenBusiestAndLeastBusyInstance);

            if (busiestInstance.concurrentTasksPerConsumer().isEmpty()) {
                return;
            }

            ConsumerTaskCount busiestConsumer = busiestInstance.concurrentTasksPerConsumer().get(0);

            for (ConsumerTaskCount consumer : busiestInstance.concurrentTasksPerConsumer()) {
                if (consumer.count() > busiestConsumer.count()) {
                    busiestConsumer = consumer;
                }
            }

            removeConsumerFromInstance(busiestInstance.instanceId(), busiestConsumer.consumerId());
        }
    }

    private int getTotalConcurrentTasks(ConsumerStatistics consumerStatistics) {
        return consumerStatistics
                .concurrentTasksPerConsumer()
                .stream()
                .filter(consumer -> !consumer.internal())
                .mapToInt(ConsumerTaskCount::count)
                .sum();
    }

    private void removeConsumerFromInstance(String instanceId, String consumerId) {
        consumerDistributor.removeConsumer(instanceId, consumerId);
        activeConsumersOnInstances.remove(new ConsumerInstanceEntry(instanceId, consumerId));
        consumersToBeDistributed.add(consumerId);
        logger.info("Consumer '{}' removed from instance '{}' due to consumer rebalance", consumerId, instanceId);
    }

    private String getLeastBusyConsumerInstanceToPlaceConsumerOn(String consumerId) {
        List<ConsumerStatistics> consumerStatistics = new ArrayList<>(consumerStatisticsPerInstance.values());
        if (consumerStatistics.isEmpty()) {
            return null;
        }

        consumerStatistics.sort(
                Comparator
                        .comparing(this::getTotalConcurrentTasks)
                        .thenComparing(ConsumerStatistics::totalTasksInQueue)
                        .thenComparing(ConsumerStatistics::totalTasksCompleted)
        );


        for (ConsumerStatistics instance : consumerStatistics) {
            if (!activeConsumersOnInstances.contains(new ConsumerInstanceEntry(instance.instanceId(), consumerId))) {
                return instance.instanceId();
            }
        }

        return null;
    }
}
