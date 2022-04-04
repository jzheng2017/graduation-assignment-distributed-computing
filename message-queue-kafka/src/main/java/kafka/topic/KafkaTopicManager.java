package kafka.topic;

import kafka.configuration.KafkaConfiguration;
import messagequeue.messagebroker.topic.FailedTopicActionException;
import messagequeue.messagebroker.topic.TopicConfiguration;
import messagequeue.messagebroker.topic.TopicManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
@Import(value = {KafkaConfiguration.class})
public class KafkaTopicManager implements TopicManager {
    private final Logger logger = LoggerFactory.getLogger(KafkaTopicManager.class);
    private final Admin admin;

    public KafkaTopicManager(Admin admin) {
        this.admin = admin;
    }

    @Override
    public void createTopic(TopicConfiguration topicConfiguration) {
        try {
            if (!topicExists(topicConfiguration.getName())) {
                KafkaTopicConfiguration kafkaTopicConfiguration = (KafkaTopicConfiguration) topicConfiguration;
                CreateTopicsResult createTopicsResult = admin.createTopics(
                        Collections.singleton(
                                new NewTopic(kafkaTopicConfiguration.getName(),
                                        kafkaTopicConfiguration.getPartitions(),
                                        (short) kafkaTopicConfiguration.getReplicationFactor())
                        ));

                createTopicsResult.values().get(kafkaTopicConfiguration.getName()).get();
                logger.info("Kafka topic created \"{}\" with {} partitions and {} replication factor", kafkaTopicConfiguration.getName(), kafkaTopicConfiguration.getPartitions(), kafkaTopicConfiguration.getReplicationFactor());
            } else {
                throw new FailedTopicActionException(String.format("The Kafka topic with the name %s already exists.", topicConfiguration.getName()));
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
}
