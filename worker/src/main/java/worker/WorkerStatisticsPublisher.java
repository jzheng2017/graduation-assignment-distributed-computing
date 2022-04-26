package worker;

import datastorage.KVClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datastorage.configuration.KeyPrefix;
import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.ConsumerTaskCount;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import worker.Worker;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class WorkerStatisticsPublisher {
    private ConsumerManager consumerManager;
    private KVClient kvClient;
    private Worker worker;
    public WorkerStatisticsPublisher(KVClient kvClient, ConsumerManager consumerManager, Worker worker) {
        this.kvClient = kvClient;
        this.consumerManager = consumerManager;
        this.worker = worker;
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

        WorkerStatistics workerStatistics = new WorkerStatistics(
                worker.getIdentifier(),
                totalTasksInQueue, totalTasksCompleted,
                totalTasksScheduled,
                concurrentTasksPerConsumerList,
                activeRunningConsumers,
                Instant.now().getEpochSecond());

        String json = new ObjectMapper().writeValueAsString(workerStatistics);
        kvClient.put(KeyPrefix.WORKER_STATISTICS + "-" + workerStatistics.workerId(), json);
    }
}
