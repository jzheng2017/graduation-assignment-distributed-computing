package messagequeue.messagebroker;

public interface MessageBrokerProxy {
    void sendMessage(String topicName, String message);
}