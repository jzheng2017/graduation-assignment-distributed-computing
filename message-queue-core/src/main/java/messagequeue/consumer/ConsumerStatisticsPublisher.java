package messagequeue.consumer;

import datastorage.KVClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datastorage.configuration.KeyPrefix;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(value = "consumer.statistics.publisher", havingValue = "on")
public class ConsumerStatisticsPublisher {
    private ConsumerManager consumerManager;
    private KVClient kvClient;

    public ConsumerStatisticsPublisher(KVClient kvClient, ConsumerManager consumerManager) {
        this.kvClient = kvClient;
        this.consumerManager = consumerManager;
    }

    @Scheduled(fixedRate = 5000L)
    public void publishStatistic() throws JsonProcessingException {
        Map<String, Integer> concurrentTasksPerConsumer = consumerManager.getTotalRunningTasksForAllConsumers();
        long totalTasksCompleted = consumerManager.getTotalNumberOfCompletedTasks();
        int totalTasksInQueue = consumerManager.getTotalNumberOfTasksInQueue();
        long totalTasksScheduled = consumerManager.getTotalNumberOfTasksScheduled();
        List<String> activeRunningConsumers = consumerManager.getAllConsumers().stream().map(Consumer::getIdentifier).toList();
        List<ConsumerTaskCount> concurrentTasksPerConsumerList = concurrentTasksPerConsumer.entrySet().stream().map(entry -> {
            String consumerId = entry.getKey();
            int taskCount = entry.getValue();
            return new ConsumerTaskCount(consumerId, taskCount, consumerManager.isConsumerInternal(consumerId));
        }).toList();

        ConsumerStatistics consumerStatistics = new ConsumerStatistics(
                consumerManager.getIdentifier(),
                totalTasksInQueue, totalTasksCompleted,
                totalTasksScheduled,
                concurrentTasksPerConsumerList,
                activeRunningConsumers,
                Instant.now().getEpochSecond());

        String json = new ObjectMapper().writeValueAsString(consumerStatistics);
        kvClient.put(KeyPrefix.WORKER_STATISTICS + "-" + consumerStatistics.instanceId(), json);
    }
}
