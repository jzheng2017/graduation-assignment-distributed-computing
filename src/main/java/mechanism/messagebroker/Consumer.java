package mechanism.messagebroker;

public interface Consumer {
    void consume();
    void acknowledgeMessage();
}
