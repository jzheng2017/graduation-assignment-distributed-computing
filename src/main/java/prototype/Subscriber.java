package prototype;

/**
 * A subscriber to a topic
 */
public interface Subscriber {
    /**
     * Reads of any topic the subscriber is subscribed to
     */
    void consume();
}
