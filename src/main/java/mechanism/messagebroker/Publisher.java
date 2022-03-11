package mechanism.messagebroker;

public interface Publisher {
    void publish(String topicName, String message);
}
