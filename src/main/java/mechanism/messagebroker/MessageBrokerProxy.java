package mechanism.messagebroker;

import java.util.List;

public abstract class MessageBrokerProxy {
    private SubscriptionManager subscriptionManager = new SubscriptionManager();

    public abstract void sendMessage(String topicName, String message);

    public String receiveMessage(String topicName, String subscriberName) {
        if (subscriptionManager.isSubscribed(topicName, subscriberName)) {
            return getMessageFromTopic(topicName, subscriberName);
        }

        throw new NotSubscribedException(String.format("%s can not receive message from %s as there is no subscription.", subscriberName, topicName));
    }

    public void subscribeToTopic(String topicName, String subscriberName) {
        subscriptionManager.subscribeToTopic(topicName, subscriberName);
    }

    public void unsubscribeToTopic(String topicName, String subscriberName) {
        subscriptionManager.subscribeToTopic(topicName, subscriberName);
    }

    public List<Subscription> getSubscriptionOfSubscriber(String subscriberName) {
        return subscriptionManager.getSubscriptions(subscriberName);
    }

    protected abstract String getMessageFromTopic(String topicName, String subscriberName);
}
