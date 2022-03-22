package messagequeue.consumer;

import messagequeue.configuration.KafkaProperties;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.messagebroker.subscription.DuplicateSubscriptionException;
import messagequeue.messagebroker.subscription.NotSubscribedException;
import messagequeue.messagebroker.subscription.Subscription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Kafka implementation of the {@link MessageProcessor} interface
 */
public abstract class BaseKafkaConsumer extends BaseConsumer {
    protected KafkaConsumer<String, String> consumer;

    //constructor only for unit test purposes
    protected BaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaConsumer<String, String> consumer) {
        super(kafkaMessageBrokerProxy, "unit test purposes");
        this.consumer = consumer;
    }

    protected BaseKafkaConsumer(KafkaMessageBrokerProxy kafkaMessageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(kafkaMessageBrokerProxy, consumerProperties.getName());
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.getGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getValueDeserializer());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.consumer = new KafkaConsumer<>(properties);

        subscribe(consumerProperties.getSubscriptions());

        new Thread(this::consume).start();
    }

    @Override
    public void consume() {
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> process(record.value()));
                acknowledge();
            } catch (MessageProcessingException ex) {
                logger.error("Processing message failed.", ex);
                throw ex;
            }
        }
    }

    @Override
    public void subscribe(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::getTopicName).collect(Collectors.toSet());

        if (!subscriptions.contains(topicName)) {
            subscriptions.add(topicName);
            consumer.subscribe(subscriptions);
        } else {
            throw new DuplicateSubscriptionException(String.format("%s is already subscribed to %s.", name, topicName));
        }
    }

    @Override
    public void subscribe(Set<String> topicList) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::getTopicName).collect(Collectors.toSet());
        subscriptions.addAll(topicList); //if there are duplicate subscriptions, then it's fine as they get left out and only leaving one in. the user doesn't need to know that as it won't have any effect on the subscription process.
        consumer.subscribe(subscriptions);
    }

    @Override
    public void unsubscribe(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::getTopicName).collect(Collectors.toSet());

        if (subscriptions.contains(topicName)) {
            subscriptions.remove(topicName);
            consumer.subscribe(subscriptions);
        } else {
            throw new NotSubscribedException(String.format("%s is not subscribed to %s.", name, topicName));
        }
    }

    @Override
    public void unsubscribe(Set<String> topicList) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::getTopicName).collect(Collectors.toSet());
        subscriptions.removeAll(topicList);
        consumer.subscribe(subscriptions); //the way this function works is that regardless if you want to subscribe or unsubscribe, you use this function, and it will update the subscriptions based on the list.
    }

    @Override
    public Set<Subscription> getSubscriptions() {
        return consumer.subscription().stream().map(Subscription::new).collect(Collectors.toSet());
    }

    @Override
    public boolean isSubscribed(String topicName) {
        Set<String> subscriptions = getSubscriptions().stream().map(Subscription::getTopicName).collect(Collectors.toSet());

        return subscriptions.contains(topicName);
    }

    @Override
    public void acknowledge() {
        consumer.commitSync();
    }
}
