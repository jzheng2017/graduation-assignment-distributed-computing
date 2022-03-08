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

    public synchronized void addMessage(String message) {
        messages.put(index++, message);
    }

    public synchronized void removeMessage(long messageId) {
        messages.remove(messageId);
    }

    public synchronized TopicMessage getNextMessage() {
        if (cursor < index) {
            String message = messages.get(cursor);
            return new TopicMessage(name, cursor++, message);
        } else {
            throw new NoTopicMessageAvailable("There are no new messages on the queue.");
        }
    }

    public synchronized void notifySuccessMessageProcessing(long messageId) {
        removeMessage(messageId);
    }

    public synchronized void notifyFailureMessageProcessing(long messageId) {
        String message = messages.get(messageId);
        removeMessage(messageId);
        addMessage(message);
    }
}
