package commons;

/**
 * A pojo holding information about an active consumer
 * @param consumerId identifier of the consumer
 * @param count the number of tasks it is consuming at the moment
 * @param internal whether the consumer is used for internal use
 */
public record ConsumerTaskCount(String consumerId, int count, boolean internal) {
}
