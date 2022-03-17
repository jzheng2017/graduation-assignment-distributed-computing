package mechanism.messagebroker;

public interface Consumer {
    void consume(String message);
    void acknowledgeMessage();
}
