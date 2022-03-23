package messagequeue.consumer;

import java.util.Set;

public class ConsumerProperties {
    private final String name;
    private final String groupId;
    private Set<String> subscriptions;

    public ConsumerProperties(String name, String groupId, Set<String> subscriptions) {
        this.name = name;
        this.groupId = groupId;
        this.subscriptions = subscriptions;
    }

    public String getName() {
        return name;
    }

    public String getGroupId() {
        return groupId;
    }

    public Set<String> getSubscriptions() {
        return subscriptions;
    }
}
