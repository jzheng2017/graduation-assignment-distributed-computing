package messagequeue.messagebroker;

public class NotSubscribedException extends RuntimeException {
    public NotSubscribedException(String message) {
        super(message);
    }
}
