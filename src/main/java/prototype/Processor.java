package prototype;

/**
 * A processor that processes the message from a topic
 */
public interface Processor {
    void processMessage(TopicMessage message);
}
