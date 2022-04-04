package coordinator;

import messagequeue.consumer.ConsumerStatistics;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerCoordinator {
    private Map<String, ConsumerStatistics> consumerStatisticsPerInstance = new ConcurrentHashMap<>();

    public void updateConsumerStatisticOfInstance(ConsumerStatistics consumerStatistics) {
        consumerStatisticsPerInstance.put(consumerStatistics.instanceId(), consumerStatistics);
    }
}
