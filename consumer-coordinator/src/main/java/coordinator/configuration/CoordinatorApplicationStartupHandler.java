package coordinator.configuration;

import coordinator.ConsumerCoordinator;
import kafka.topic.KafkaTopicConfiguration;
import messagequeue.messagebroker.MessageBrokerProxy;
import messagequeue.messagebroker.topic.TopicManager;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class CoordinatorApplicationStartupHandler implements ApplicationRunner {
    private ConsumerCoordinator consumerCoordinator;
    private TopicManager topicManager;
    private MessageBrokerProxy messageBrokerProxy;
    public CoordinatorApplicationStartupHandler(ConsumerCoordinator consumerCoordinator, TopicManager topicManager, MessageBrokerProxy messageBrokerProxy) {
        this.consumerCoordinator = consumerCoordinator;
        this.topicManager = topicManager;
        this.messageBrokerProxy = messageBrokerProxy;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        topicManager.createTopic(new KafkaTopicConfiguration("input", 1, (short)1));
        topicManager.createTopic(new KafkaTopicConfiguration("output", 1, (short)1));
        topicManager.createTopic(new KafkaTopicConfiguration("reversed", 1, (short)1));
//        for (int i = 0; i < 1000; i++) {
//            messageBrokerProxy.sendMessage("input", "a");
//        }
        final String upperCaseJson = "{\n" +
                " \"name\": \"uppercase\",\n" +
                " \"groupId\": \"uppercase1\",\n" +
                " \"subscriptions\": [\"input\"],\n" +
                "}";
        final String reverserJson = "{\n" +
                " \"name\": \"reverser\",\n" +
                " \"groupId\": \"reverser1\",\n" +
                " \"subscriptions\": [\"output\"],\n" +
                "}";
        final String printerJson = "{\n" +
                " \"name\": \"printer\",\n" +
                " \"groupId\": \"printer1\",\n" +
                " \"subscriptions\": [\"reversed\"],\n" +
                "}";
        consumerCoordinator.addConsumerConfiguration(upperCaseJson);
        consumerCoordinator.addConsumerConfiguration(reverserJson);
        consumerCoordinator.addConsumerConfiguration(printerJson);
    }
}
