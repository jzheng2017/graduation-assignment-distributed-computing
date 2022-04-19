package messagequeue.consumer;

public record ConsumerTaskCount(String consumerId, int count, boolean internal) {
}
