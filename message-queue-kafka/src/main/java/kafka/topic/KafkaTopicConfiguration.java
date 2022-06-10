package kafka.topic;

import messagequeue.messagebroker.topic.TopicConfiguration;

public class KafkaTopicConfiguration extends TopicConfiguration {
    private final int partitions;
    private final short replicationFactor;

    public KafkaTopicConfiguration(String name, int partitions, short replicationFactor) {
        super(name);
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public int getPartitions() {
        return partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }
}
