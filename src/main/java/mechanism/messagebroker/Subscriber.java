package mechanism.messagebroker;

public interface Subscriber {
    void subscribe(String topicName);
    void unsubscribe(String topicName);
}
