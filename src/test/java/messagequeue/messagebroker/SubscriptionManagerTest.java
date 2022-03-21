package messagequeue.messagebroker;

import messagequeue.messagebroker.subscription.Subscription;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class SubscriptionManagerTest {
    private SubscriptionManager subscriptionManager;
    final String topicName = "test";
    final String subscriberName = "subscriber1";

    @BeforeEach
    void setup() {
        subscriptionManager = new SubscriptionManager();
    }

    @Test
    void testThatSubscribingWorksCorrectly() {
        subscriptionManager.subscribeToTopic(topicName, subscriberName);

        List<Subscription> subscriptions = subscriptionManager.getSubscriptions(subscriberName);

        Assertions.assertTrue(subscriptions.contains(new Subscription(topicName, subscriberName)));
    }

    @Test
    void testThatUnsubscribingWorksCorrectly() {
        testThatSubscribingWorksCorrectly();
        subscriptionManager.unsubscribeToTopic(topicName, subscriberName);
        List<Subscription> subscriptions = subscriptionManager.getSubscriptions(subscriberName);

        Assertions.assertTrue(subscriptions.isEmpty());
    }

    @Test
    void testThatCheckingForSubscriptionWorksCorrectly() {
        subscriptionManager.subscribeToTopic(topicName, subscriberName);
        Assertions.assertTrue(subscriptionManager.isSubscribed(topicName, subscriberName));
        Assertions.assertFalse(subscriptionManager.isSubscribed("a fake topic name", subscriberName));
    }
}
