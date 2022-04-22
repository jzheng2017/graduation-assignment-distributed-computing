package coordinator.dto;

import java.util.List;

public record ConsumerStatistics(String workerId, int totalTasksInQueue, long totalTasksCompleted, long totalTasksScheduled, List<ConsumerTaskCount> concurrentTasksPerConsumer, List<String> activeConsumers, long timestamp) {
}