package messagequeue.consumer;

import java.util.Map;

public record ConsumerStatistics(String instanceId, int totalTasksInQueue, long totalTasksCompleted, long totalTasksScheduled, Map<String, Integer> concurrentTasksPerConsumer) {
}