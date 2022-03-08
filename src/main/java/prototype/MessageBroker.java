package prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MessageBroker {
    private static final int TIME_BETWEEN_POLL_IN_MS = 500;
    private Map<String, Topic> topics = new HashMap<>();
    private Map<String, Set<Subscriber>> topicSubscribers = new HashMap<>();

    public void subscribeToTopic(String topicName, Subscriber subscriber) {
        if (topicSubscribers.containsKey(topicName)) {
            topicSubscribers.get(topicName).add(subscriber);
        } else {
            Set<Subscriber> subscribers = new HashSet<>();
            subscribers.add(subscriber);
            topicSubscribers.put(topicName, subscribers);
        }
    }

    public void unsubscribeToTopic(String topicName, Subscriber subscriber) {
        if (topicSubscribers.containsKey(topicName)) {
            Set<Subscriber> subscribers = topicSubscribers.get(topicName);
            if (subscribers.contains(subscriber)) {
                topicSubscribers.get(topicName).remove(subscriber);
            } else {
                throw new SubscriberNotFoundException(String.format("The given subscriber is not subscribed to the topic %s", topicName));
            }
        } else {
            throw new TopicNotFoundException(String.format("Topic with the name %s doesn't exist.", topicName));
        }
    }

    public void addTopic(String topicName) {
        topics.put(topicName, new Topic(topicName));
    }

    public void removeTopic(String topicName) {
        topics.remove(topicName);
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public void addMessage(String topicName, String message) {
        if (topics.containsKey(topicName)) {
            topics.get(topicName).addMessage(message);
        } else {
            throw new TopicNotFoundException(String.format("Topic with the name %s doesn't exist.", topicName));
        }
    }

    public void removeMessage(String topicName, long messageId) {
        if (topics.containsKey(topicName)) {
            topics.get(topicName).removeMessage(messageId);
        }
    }

    public TopicMessage getNextMessageForSubscriber(Subscriber subscriber) {
        while (true) {
            List<Topic> allSubscribedTopics = getAllTopicSubscriptionsOfSubscriber(subscriber);
            for (Topic topic : allSubscribedTopics) {
                TopicMessage message;
                try {
                    message = topic.getNextMessage();
                    if (message != null) {
                        return message;
                    }
                } catch (NoTopicMessageAvailable ex) {
                    continue;
                }
            }
            try {
                Thread.sleep(TIME_BETWEEN_POLL_IN_MS);
            } catch (InterruptedException ex) {
                System.out.println("Failed to sleep. Cause: " + ex.getMessage());
            }
        }
    }

    private List<Topic> getAllTopicSubscriptionsOfSubscriber(Subscriber subscriber) {
        List<Topic> allSubscribedTopics = new ArrayList<>();

        for (Map.Entry<String, Set<Subscriber>> topicSubscription : topicSubscribers.entrySet()) {
            if (topicSubscription.getValue().contains(subscriber)) {
                allSubscribedTopics.add(topics.get(topicSubscription.getKey()));
            }
        }
        return allSubscribedTopics;
    }

    public void notifySuccessMessageProcessing(String topicName, long messageId) {
        if (topics.containsKey(topicName)) {
            topics.get(topicName).notifySuccessMessageProcessing(messageId);
        }
    }

    public void notifyFailureMessageProcessing(String topicName, long messageId) {
        if (topics.containsKey(topicName)) {
            topics.get(topicName).notifyFailureMessageProcessing(messageId);
        }
    }
}
