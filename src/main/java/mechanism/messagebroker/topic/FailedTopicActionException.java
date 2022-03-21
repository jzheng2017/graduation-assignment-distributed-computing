package mechanism.messagebroker.topic;

public class FailedTopicActionException extends RuntimeException {
    public FailedTopicActionException(String message) {
        super(message);
    }

    public FailedTopicActionException(String message, Throwable t) {
        super(message, t);
    }
}
