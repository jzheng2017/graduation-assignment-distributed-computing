package coordinator.dto;

public record ConsumerTaskCount(String consumerId, int count, boolean internal) {
}
