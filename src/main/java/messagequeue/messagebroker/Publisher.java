package messagequeue.messagebroker;

public interface Publisher {
    void publish(String topicName, String message);
}
