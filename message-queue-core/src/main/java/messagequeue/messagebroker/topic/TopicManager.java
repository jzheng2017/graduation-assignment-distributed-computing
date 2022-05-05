package messagequeue.messagebroker.topic;

import messagequeue.consumer.TopicOffset;

import java.util.List;
import java.util.Map;

/**
 * An interface allows one to manage topics
 */
public interface TopicManager {
    void createTopic(TopicConfiguration topicConfiguration);

    void removeTopic(String topicName);

    List<String> getTopics();

    boolean topicExists(String topicName);

    void updateTopicOffsets(List<TopicOffset> topicOffsets, Map<String, Object> consumerContext);
}
