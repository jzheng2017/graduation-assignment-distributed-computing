package messagequeue.configuration;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class EnvironmentSetup {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private final boolean isConsumerInstance;
    private ConsumerManager consumerManager;
    private KVClient kvClient;
    public static final int NUMBER_OF_PARTITIONS = 3;


    public EnvironmentSetup(boolean isConsumerInstance, ConsumerManager consumerManager, KVClient kvClient) {
        this.isConsumerInstance = isConsumerInstance;
        this.consumerManager = consumerManager;
        this.kvClient = kvClient;
    }

    public void setup() {
        if (isConsumerInstance) {
            registerWorker();
        } else {
            createPartitions();
        }
    }

    private void registerWorker() {
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + consumerManager.getIdentifier(), Long.toString(Instant.now().getEpochSecond()));
        logger.info("Registered worker '{}'", consumerManager.getIdentifier());
    }

    private void createPartitions() {
        kvClient.put(KeyPrefix.PARTITION_COUNT, Integer.toString(NUMBER_OF_PARTITIONS));
        logger.info("Created '{}' partitions", NUMBER_OF_PARTITIONS);
    }
}
