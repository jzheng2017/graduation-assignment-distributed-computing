package messagequeue.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import messagequeue.consumer.Consumer;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.internal.InternalConsumerBuilder;
import messagequeue.messagebroker.MessageBrokerProxy;
import messagequeue.messagebroker.topic.TopicConfiguration;
import messagequeue.messagebroker.topic.TopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class EnvironmentSetup {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private final boolean isConsumerInstance;
    private ConsumerManager consumerManager;
    private InternalConsumerBuilder consumerBuilder;
    private MessageBrokerProxy messageBrokerProxy;
    private TopicManager topicManager;


    public EnvironmentSetup(boolean isConsumerInstance, ConsumerManager consumerManager, InternalConsumerBuilder consumerBuilder, MessageBrokerProxy messageBrokerProxy, TopicManager topicManager) {
        this.isConsumerInstance = isConsumerInstance;
        this.consumerManager = consumerManager;
        this.consumerBuilder = consumerBuilder;
        this.messageBrokerProxy = messageBrokerProxy;
        this.topicManager = topicManager;
    }

    public void setup() {
        if (isConsumerInstance) {
            setupConsumerInstanceInternalTopics();
            setupConsumerInstanceInternalConsumers();
            registerInstanceToCoordinator();
        } else {
            setupCoordinatorInternalTopics();
            setupCoordinatorInternalConsumers();
        }
    }

    private void registerInstanceToCoordinator() {
        try {
            messageBrokerProxy.sendMessage("consumer-registration", new ObjectMapper().writeValueAsString(new Registration("register", consumerManager.getIdentifier())));
            logger.info("Send registration request to coordinator with instanceId '{}'", consumerManager.getIdentifier());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Can not connect to coordinator");
        }
    }

    private void setupConsumerInstanceInternalTopics() {
        final String consumerTopic = consumerManager.getIdentifier() + "-consumers";
        topicManager.createTopic(new TopicConfiguration(consumerTopic));
    }

    private void setupConsumerInstanceInternalConsumers() {
        consumerManager.registerConsumer(
                consumerBuilder.createConsumer(
                        new ConsumerProperties(
                                "consumer-manager",
                                consumerManager.getIdentifier() + "-consumer-manager",
                                Set.of(consumerManager.getIdentifier() + "-consumers"),
                                2)
                )
        );
        logger.info("Registered internal consumers");
    }

    private void setupCoordinatorInternalTopics() {
        final List<String> topics = List.of("consumer-statistics", "consumer-registration");

        for (String topic : topics) {
            topicManager.createTopic(new TopicConfiguration(topic));
        }
        logger.info("Created internal topics");
    }

    private void setupCoordinatorInternalConsumers() {
        List<ConsumerProperties> consumerProperties = List.of(
                new ConsumerProperties("consumer-statistics", "statistics", Set.of("consumer-statistics"), 1),
                new ConsumerProperties("consumer-registration", "registration", Set.of("consumer-registration"), 1)
        );

        for (ConsumerProperties consumerProperty : consumerProperties) {
            Consumer consumer = consumerBuilder.createConsumer(consumerProperty);
            consumerManager.registerConsumer(consumer);
        }
        logger.info("Registered internal consumers");
    }

    private record Registration(String registrationType, String instanceId) {
    }
}
