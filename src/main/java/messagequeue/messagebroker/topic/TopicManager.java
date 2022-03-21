package messagequeue.messagebroker.topic;

import java.util.List;

public interface TopicManager {
    void createTopic(TopicConfiguration topicConfiguration);
    void removeTopic(String topicName);
    List<Topic> getTopics();
    boolean topicExists(String topicName);
}
