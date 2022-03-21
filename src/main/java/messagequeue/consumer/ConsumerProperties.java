package messagequeue.consumer;

import java.util.ArrayList;
import java.util.List;

public class ConsumerProperties {
    private String name;
    private String groupId;
    private List<String> subscriptions = new ArrayList<>();

    public ConsumerProperties(String name, String groupId, List<String> subscriptions) {
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

    public List<String> getSubscriptions() {
        return subscriptions;
    }
}
