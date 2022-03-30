package impl.configuration;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.ConsumerManagerImpl;
import messagequeue.consumer.builder.JsonConsumerConfigurationParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Import(value = {ConsumerManagerImpl.class, ConsumerBuilder.class, JsonConsumerConfigurationParser.class})
@Component
public class ApplicationStartupHandler implements ApplicationRunner {
    //    @Autowired
//    private KafkaTopicManager kafkaTopicManager;
    @Autowired
    private ConsumerManagerImpl consumerManager;

    private final String messageForwarderConsumerConfigurationJson = "{\n" +
            "\t\"name\": \"forwarder\",\n" +
            "\t\"groupId\": \"forwarder\",\n" +
            "\t\"subscriptions\": [\n" +
            "\t\t\"input\"\n" +
            "\t]\n" +
            "}";

    private final String messagePrinterConsumerConfigurationJson = "{\n" +
            "\t\"name\": \"printer\",\n" +
            "\t\"groupId\": \"printer1\",\n" +
            "\t\"subscriptions\": [\n" +
            "\t\t\"reversed\"\n" +
            "\t]\n" +
            "}";

    private final String messageReverserConsumerConfigurationJson = "{\n" +
            "\t\"name\": \"reverser\",\n" +
            "\t\"groupId\": \"reverser1\",\n" +
            "\t\"subscriptions\": [\n" +
            "\t\t\"output\"\n" +
            "\t]\n" +
            "}";

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("input", 1, 1));
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("output", 1, 1));
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("reversed", 1, 1));

        //for testing purposes
        consumerManager.registerConsumer(messagePrinterConsumerConfigurationJson);
        consumerManager.registerConsumer(messageForwarderConsumerConfigurationJson);
        consumerManager.registerConsumer(messageReverserConsumerConfigurationJson);
//        Thread.sleep(10000);
//        consumerManager.unregisterConsumer("printer");
//        Thread.sleep(10000);
//        consumerManager.registerConsumer(messagePrinterConsumerConfigurationJson);
    }
}
