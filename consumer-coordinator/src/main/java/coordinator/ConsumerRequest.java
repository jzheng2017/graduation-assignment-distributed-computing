package coordinator;

import messagequeue.consumer.ConsumerProperties;

public record ConsumerRequest(String actionType, ConsumerProperties consumer) {
}
