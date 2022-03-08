package prototype;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    private final String name;
    private Map<Long, String> messages = new HashMap<>();
    private long cursor = Long.MIN_VALUE;
    private long index = Long.MIN_VALUE;

    public Topic(String name) {
        this.name = name;
    }

    public void addMessage(String message) {
        messages.put(index++, message);
    }

    public void removeMessage(long messageId) {
        messages.remove(messageId);
    }

    public TopicMessage getNextMessage() {
        if (cursor < index) {
            String message = messages.get(cursor);
            return new TopicMessage(name, cursor++, message);
        } else {
            throw new NoTopicMessageAvailable("There are no new messages on the queue.");
        }
    }

    public void notifySuccessMessageProcessing(long messageId) {
        removeMessage(messageId);
    }

    public void notifyFailureMessageProcessing(long messageId) {
        String message = messages.get(messageId);
        removeMessage(messageId);
        addMessage(message);
    }
}
