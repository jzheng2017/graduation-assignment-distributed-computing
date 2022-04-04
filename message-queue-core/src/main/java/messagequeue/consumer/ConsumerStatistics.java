package messagequeue.consumer;

import java.util.Map;

public record ConsumerStatistics(String instanceId, Map<String, Integer> concurrentTasksPerConsumer) {
}