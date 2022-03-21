package messagequeue.messagebroker.subscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionManager {
    private Map<String, Set<Subscription>> subscriptions = new ConcurrentHashMap<>();

    public void subscribeToTopic(String topicName, String subscriberName) {
        if (subscriptions.containsKey(subscriberName)) {
            Set<Subscription> subscriptionsOfSubscriber = subscriptions.get(subscriberName);

            if (!subscriptionsOfSubscriber.add(new Subscription(topicName, subscriberName))) {
                throw new DuplicateSubscriptionException(String.format("%s is already subscribed to %s.", subscriberName, topicName));
            }
        } else {
            Set<Subscription> subscriptionsOfSubscriber = new HashSet<>();
            subscriptionsOfSubscriber.add(new Subscription(topicName, subscriberName));
            subscriptions.put(subscriberName, subscriptionsOfSubscriber);
        }
    }

    public void unsubscribeToTopic(String topicName, String subscriberName) {
        if (subscriptions.containsKey(subscriberName)) {
            Set<Subscription> subscriptionsOfSubscriber = subscriptions.get(subscriberName);
            if (!subscriptionsOfSubscriber.remove(new Subscription(topicName, subscriberName))) {
                throw new SubscriptionNotFoundException(String.format("%s is not subscribed to %s.", subscriberName, topicName));
            }
        } else {
            throw new SubscriptionNotFoundException(String.format("%s does not have any subscriptions!", subscriberName));
        }
    }

    public boolean isSubscribed(String topicName, String subscriberName) {
        return subscriptions.containsKey(subscriberName) && subscriptions.get(subscriberName).contains(new Subscription(topicName, subscriberName));
    }

    public List<Subscription> getSubscriptions(String subscriberName) {
        Set<Subscription> subscriptionsOfSubscriber = subscriptions.get(subscriberName);
        if (subscriptionsOfSubscriber == null) {
            return new ArrayList<>();
        }

        return Collections.unmodifiableList(subscriptionsOfSubscriber.stream().toList());
    }
}
