package worker;

import commons.ConsumerTaskCount;
import commons.WorkerStatistics;
import datastorage.KVClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.KeyPrefix;
import messagequeue.consumer.ConsumerManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * This publisher class is responsible for publish the execution statistics (concurrent tasks being processed, tasks in queue, total processed, etc) of this worker.
 */
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
    public void publishStatistic() throws JsonProcessingException, ExecutionException, InterruptedException {
        Map<String, Integer> concurrentTasksPerConsumer = consumerManager.getTotalRunningTasksForAllConsumers();
        long totalTasksCompleted = consumerManager.getTotalNumberOfCompletedTasks();
        int totalTasksInQueue = consumerManager.getTotalNumberOfTasksInQueue();
        List<String> activeRunningConsumers = consumerManager.getAllConsumers();
        List<ConsumerTaskCount> concurrentTasksPerConsumerList = concurrentTasksPerConsumer
                .entrySet()
                .stream()
                .map(entry -> {
                    String consumerId = entry.getKey();
                    int taskCount = entry.getValue();
                    return new ConsumerTaskCount(consumerId, taskCount, consumerManager.isConsumerInternal(consumerId));
                })
                .toList();

        WorkerStatistics workerStatistics = new WorkerStatistics(
                worker.getIdentifier(),
                worker.getAssignedPartition(),
                totalTasksInQueue,
                totalTasksCompleted,
                concurrentTasksPerConsumerList,
                activeRunningConsumers,
                Instant.now().getEpochSecond());

        String json = new ObjectMapper().writeValueAsString(workerStatistics);
        kvClient.put(KeyPrefix.WORKER_STATISTICS + "-" + workerStatistics.workerId(), json).get();
    }
}
