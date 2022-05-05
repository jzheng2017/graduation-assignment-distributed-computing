package kafka.topic;

import kafka.configuration.KafkaProperties;
import messagequeue.consumer.TopicOffset;
import messagequeue.messagebroker.topic.FailedTopicActionException;
import messagequeue.messagebroker.topic.TopicConfiguration;
import messagequeue.messagebroker.topic.TopicManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * This class is an implementation of the {@link TopicManager} interface. It allows one to create and manage Kafka topics.
 */
@Service
public class KafkaTopicManager implements TopicManager {
    private final Logger logger = LoggerFactory.getLogger(KafkaTopicManager.class);
    private final Admin admin;
    private static final int DEFAULT_NUM_PARTITIONS = 2;
    private static final short DEFAULT_NUM_REPLICATIONS = 1;

    //for unit test purposes
    protected KafkaTopicManager(Admin admin) {
        this.admin = admin;
    }

    @Autowired
    public KafkaTopicManager(KafkaProperties kafkaProperties) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        admin = Admin.create(props);
    }

    @Override
    public void createTopic(TopicConfiguration topicConfiguration) {
        try {
            if (!topicExists(topicConfiguration.getName())) {
                String name;
                int partitions;
                short replication;

                if (topicConfiguration instanceof KafkaTopicConfiguration kafkaTopicConfiguration) {
                    name = kafkaTopicConfiguration.getName();
                    partitions = kafkaTopicConfiguration.getPartitions();
                    replication = kafkaTopicConfiguration.getReplicationFactor();
                } else {
                    name = topicConfiguration.getName();
                    partitions = DEFAULT_NUM_PARTITIONS;
                    replication = DEFAULT_NUM_REPLICATIONS;
                }

                CreateTopicsResult createTopicsResult = admin.createTopics(
                        Collections.singleton(
                                new NewTopic(name, partitions, replication)
                        ));

                createTopicsResult.values().get(name).get();
                logger.info("Kafka topic created \"{}\" with {} partitions and {} replication factor", name, partitions, replication);
            } else {
                logger.warn("The Kafka topic with the name {} already exists.", topicConfiguration.getName());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new FailedTopicActionException(String.format("Creating Kafka topic with the name %s failed.", topicConfiguration.getName()), e);
        }
    }

    @Override
    public void removeTopic(String topicName) {
        try {
            DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(Collections.singletonList(topicName));
            deleteTopicsResult.topicNameValues().get(topicName).get();
            logger.info("Kafka topic with the name {} deleted", topicName);
        } catch (InterruptedException | ExecutionException e) {
            throw new FailedTopicActionException(String.format("Could not delete topic with the name %s", topicName), e);
        }
    }

    @Override
    public List<String> getTopics() {
        try {
            ListTopicsResult listTopicsResult = admin.listTopics();
            return listTopicsResult.listings().get().stream().map(topic -> topic.name()).toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new FailedTopicActionException("Getting all topics failed", e);
        }
    }

    @Override
    public boolean topicExists(String topicName) {
        List<String> topics = getTopics();
        return topics.contains(topicName);
    }

    @Override
    public void updateTopicOffsets(List<TopicOffset> topicOffsets, Map<String, Object> consumerContext) {
        KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerContext.get("consumer");
        topicOffsets.forEach(topicOffset -> consumer.seek(new TopicPartition(topicOffset.topic(), 0), topicOffset.offset()));
    }
}
