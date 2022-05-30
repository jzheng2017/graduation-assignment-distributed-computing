package commons;

/**
 * A pojo holding information about an active consumer
 * @param consumerId identifier of the consumer
 * @param concurrentTaskCount the number of tasks it is consuming at the moment
 * @param remainingTasksInQueue the number of remaining tasks that are in queue
 * @param internal whether the consumer is used for internal use
 */
public record ConsumerTaskCount(String consumerId, int concurrentTaskCount, int remainingTasksInQueue, boolean internal) {
}
