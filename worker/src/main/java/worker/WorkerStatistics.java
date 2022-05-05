package worker;

import messagequeue.consumer.ConsumerTaskCount;

import java.util.List;

public record WorkerStatistics(String workerId, int partition, int totalTasksInQueue, long totalTasksCompleted, List<ConsumerTaskCount> concurrentTasksPerConsumer, List<String> activeConsumers, long timestamp) {
}