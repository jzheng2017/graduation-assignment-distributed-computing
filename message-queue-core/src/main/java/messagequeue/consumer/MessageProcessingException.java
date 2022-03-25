package messagequeue.consumer;

public class MessageProcessingException extends RuntimeException {
    public MessageProcessingException(String message) {
        super(message);
    }
}
