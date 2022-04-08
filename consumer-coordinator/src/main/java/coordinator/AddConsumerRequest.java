package coordinator;

import messagequeue.consumer.ConsumerProperties;

public record AddConsumerRequest(String actionType, ConsumerProperties consumer) {
}
