package messagequeue.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConsumerProperties {
    private final String name;
    private final String groupId;
    private Set<String> subscriptions;
    private int threadPoolSize;

    public ConsumerProperties(String name, String groupId, Set<String> subscriptions, int threadPoolSize) {
        this.name = name;
        this.groupId = groupId;
        this.subscriptions = subscriptions;
        this.threadPoolSize = threadPoolSize;
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

    public int getThreadPoolSize() {
        return threadPoolSize;
    }
}
