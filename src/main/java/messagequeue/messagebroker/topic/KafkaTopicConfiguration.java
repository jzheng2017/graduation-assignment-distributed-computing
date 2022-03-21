package messagequeue.messagebroker.topic;

public class KafkaTopicConfiguration extends TopicConfiguration{
    private int partitions;
    private int replicationFactor;
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
