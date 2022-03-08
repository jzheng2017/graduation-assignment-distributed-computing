package prototype;

/**
 * A class that implements this interface has the ability to publish messages to a topic
 */
public interface Publisher {
    void publish(String topicName, String message);
}
