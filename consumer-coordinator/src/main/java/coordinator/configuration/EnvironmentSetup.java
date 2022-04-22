package coordinator.configuration;

import coordinator.ConsumerCoordinator;
import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentSetup implements ApplicationRunner {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private ConsumerCoordinator consumerCoordinator;
    private KVClient kvClient;
    public static final int NUMBER_OF_PARTITIONS = 3;

    public EnvironmentSetup(ConsumerCoordinator consumerCoordinator, KVClient kvClient) {
        this.consumerCoordinator = consumerCoordinator;
        this.kvClient = kvClient;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        final String upperCaseJson = "{\n" +
//                " \"name\": \"uppercase\",\n" +
//                " \"groupId\": \"uppercase1\",\n" +
//                " \"subscriptions\": [\"input\"]\n" +
//                "}";
//        final String reverserJson = "{\n" +
//                " \"name\": \"reverser\",\n" +
//                " \"groupId\": \"reverser1\",\n" +
//                " \"subscriptions\": [\"output\"]\n" +
//                "}";
//        final String printerJson = "{\n" +
//                " \"name\": \"printer\",\n" +
//                " \"groupId\": \"printer1\",\n" +
//                " \"subscriptions\": [\"reversed\"]\n" +
//                "}";
//        consumerCoordinator.addConsumerConfiguration(upperCaseJson);
//        consumerCoordinator.addConsumerConfiguration(reverserJson);
//        consumerCoordinator.addConsumerConfiguration(printerJson);
        createPartitions();
    }

    private void createPartitions() {
        kvClient.put(KeyPrefix.PARTITION_COUNT, Integer.toString(NUMBER_OF_PARTITIONS))
                .thenAcceptAsync(ignore -> logger.info("Created '{}' partitions", NUMBER_OF_PARTITIONS));
    }
}
