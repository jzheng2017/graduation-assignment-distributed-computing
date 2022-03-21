package mechanism.messageprocessor;

import mechanism.configuration.KafkaProperties;
import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.Subscription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;

/**
 * A Kafka implementation of the {@link MessageProcessor} interface
 */
public abstract class BaseKafkaConsumer extends BaseConsumer {
    private KafkaConsumer<String, String> consumer;

    protected BaseKafkaConsumer(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, ConsumerProperties consumerProperties) {
        super(messageBrokerProxy, consumerProperties.getName());
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getHostUrl());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.getGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getValueDeserializer());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        consumerProperties.getSubscriptions().forEach(this::subscribe);
        new Thread(this::consume).start();
    }

    @Override
    public void consume() {
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> process(record.value()));
                acknowledgeMessage();
            } catch (MessageProcessingException ex) {
                logger.error("Processing message failed.", ex);
                throw ex;
            }
        }
    }

    @Override
    public void registerSubscription(String topicName) {
        consumer.subscribe(messageBrokerProxy.getSubscriptionOfSubscriber(name)
                .stream()
                .map(Subscription::getTopicName)
                .toList());
    }

    @Override
    public void acknowledgeMessage() {

    }
}
