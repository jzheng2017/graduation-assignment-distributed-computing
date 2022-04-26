package messagequeue.consumer;

import java.util.List;

public record WorkerStatistics(String workerId, int totalTasksInQueue, long totalTasksCompleted, long totalTasksScheduled, List<ConsumerTaskCount> concurrentTasksPerConsumer, List<String> activeConsumers, long timestamp) {
}