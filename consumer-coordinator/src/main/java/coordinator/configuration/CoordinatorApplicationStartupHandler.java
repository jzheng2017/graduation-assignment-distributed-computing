package coordinator.configuration;

import coordinator.processor.builder.KafkaConsumerFactory;
import kafka.subscription.KafkaSubscriptionManager;
import messagequeue.consumer.ConsumerManager;
import messagequeue.consumer.ConsumerManagerImpl;
import messagequeue.consumer.builder.JsonConsumerConfigurationParser;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Component
@Import(value = {ConsumerManagerImpl.class, KafkaSubscriptionManager.class, JsonConsumerConfigurationParser.class, KafkaConsumerFactory.class})
public class CoordinatorApplicationStartupHandler implements ApplicationRunner {
    private String consumerUpdaterConsumerJson = "{\n" +
            "\t\"name\": \"consumer-statistics-updater\",\n" +
            "\t\"groupId\": \"update-statistics\",\n" +
            "\t\"subscriptions\": [\n" +
            "\t\t\"consumer-statistics\"\n" +
            "\t]\n" +
            "}";
//    @Autowired
    private ConsumerManager consumerManager;

    public CoordinatorApplicationStartupHandler(ConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        consumerManager.registerConsumer(consumerUpdaterConsumerJson);
    }
}
