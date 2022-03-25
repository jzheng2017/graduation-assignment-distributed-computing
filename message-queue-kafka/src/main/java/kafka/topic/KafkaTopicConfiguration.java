package kafka.topic;

import messagequeue.messagebroker.topic.TopicConfiguration;

public class KafkaTopicConfiguration extends TopicConfiguration {
    private final int partitions;
    private final int replicationFactor;

    public KafkaTopicConfiguration(String name, int partitions, int replicationFactor) {
        super(name);
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }
}
