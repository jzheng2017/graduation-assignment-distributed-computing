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
    private EnvironmentConfiguration environmentConfiguration;
    private PartitionManager partitionManager;

    public EnvironmentSetup(EnvironmentConfiguration environmentConfiguration, PartitionManager partitionManager) {
        this.environmentConfiguration = environmentConfiguration;
        this.partitionManager = partitionManager;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        createPartitions();
    }

    private void createPartitions() {
        final int numberOfPartitions = environmentConfiguration.getPartitions();
        partitionManager.createPartitions(numberOfPartitions);
    }
}
