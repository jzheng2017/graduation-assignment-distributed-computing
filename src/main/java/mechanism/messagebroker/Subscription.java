package mechanism.messagebroker;

import java.util.Objects;

public class Subscription {
    private String topicName;
    private String subscriberName;

    public Subscription(String topicName, String subscriberName) {
        this.topicName = topicName;
        this.subscriberName = subscriberName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(topicName, that.topicName) && Objects.equals(subscriberName, that.subscriberName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, subscriberName);
    }
}
