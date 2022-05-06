package commons;

import java.util.List;

/**
 * A pojo holding task executions information for a certain running worker
 * @param workerId identifier of the worker
 * @param partition the partition it is responsible for
 * @param totalTasksInQueue the number of tasks it has in the queue currently
 * @param totalTasksCompleted the total number of tasks it has completed
 * @param concurrentTasksPerConsumer the number of tasks each consumer is processing concurrently
 * @param activeConsumers the consumers running on the worker
 * @param timestamp the time these statistics were recorded
 */
public record WorkerStatistics(String workerId, int partition, int totalTasksInQueue, long totalTasksCompleted, List<ConsumerTaskCount> concurrentTasksPerConsumer, List<String> activeConsumers, long timestamp) {
}