package messagequeue.consumer;

public record TaskPackageResult(String topic, boolean successful, int totalProcessed) {
}
