package coordinator.configuration;

import coordinator.ConsumerCoordinator;
import coordinator.partition.PartitionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentSetup implements ApplicationRunner {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private ConsumerCoordinator consumerCoordinator;
    private EnvironmentConfiguration environmentConfiguration;
    private PartitionManager partitionManager;

    public EnvironmentSetup(ConsumerCoordinator consumerCoordinator, EnvironmentConfiguration environmentConfiguration, PartitionManager partitionManager) {
        this.consumerCoordinator = consumerCoordinator;
        this.environmentConfiguration = environmentConfiguration;
        this.partitionManager = partitionManager;
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
        final int numberOfPartitions = environmentConfiguration.getPartitions();
        partitionManager.createPartitions(numberOfPartitions);
    }
}
