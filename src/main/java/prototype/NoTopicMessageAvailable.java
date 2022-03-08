package prototype;

public class NoTopicMessageAvailable extends RuntimeException {
    public NoTopicMessageAvailable(String message) {
        super(message);
    }
}
