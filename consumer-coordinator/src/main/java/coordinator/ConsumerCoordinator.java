package coordinator;

import messagequeue.consumer.ConsumerStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerCoordinator {
    private Logger logger = LoggerFactory.getLogger(ConsumerCoordinator.class);
    private Map<String, ConsumerStatistics> consumerStatisticsPerInstance = new ConcurrentHashMap<>();
    private Set<String> registeredInstanceIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private ConsumerDistributor consumerDistributor;

    public ConsumerCoordinator(ConsumerDistributor consumerDistributor) {
        this.consumerDistributor = consumerDistributor;
    }

    public void updateConsumerStatisticOfInstance(ConsumerStatistics consumerStatistics) {
        consumerStatisticsPerInstance.put(consumerStatistics.instanceId(), consumerStatistics);
    }

    public void registerInstance(String instanceId) {
        this.registeredInstanceIds.add(instanceId);
        logger.info("Registered instance with id '{}'", instanceId);
        deploy();
    }

    public void unregisterInstance(String instanceId) {
        this.registeredInstanceIds.remove(instanceId);
        logger.info("Unregistered instance with id '{}'", instanceId);
    }

    public void deploy() {
        consumerDistributor.distribute(registeredInstanceIds);
    }
}
