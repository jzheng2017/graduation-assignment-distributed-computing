package commons;

import java.util.Set;

public record ConsumerProperties(String name, String groupId, Set<String> subscriptions) {
}
