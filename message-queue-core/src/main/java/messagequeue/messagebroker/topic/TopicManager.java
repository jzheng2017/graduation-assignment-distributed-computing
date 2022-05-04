package messagequeue.messagebroker.topic;

import java.util.List;

/**
 * An interface allows one to manage topics
 */
public interface TopicManager {
    void createTopic(TopicConfiguration topicConfiguration);

    void removeTopic(String topicName);

    List<String> getTopics();

    boolean topicExists(String topicName);
}
