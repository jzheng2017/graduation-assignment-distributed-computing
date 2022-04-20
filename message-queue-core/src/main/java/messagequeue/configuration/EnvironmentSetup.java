package messagequeue.configuration;

import datastorage.KVClient;
import datastorage.configuration.EtcdKeyPrefix;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class EnvironmentSetup {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private final boolean isConsumerInstance;
    private ConsumerManager consumerManager;
    private KVClient kvClient;


    public EnvironmentSetup(boolean isConsumerInstance, ConsumerManager consumerManager, KVClient kvClient) {
        this.isConsumerInstance = isConsumerInstance;
        this.consumerManager = consumerManager;
        this.kvClient = kvClient;
    }

    public void setup() {
        if (isConsumerInstance) {
            registerWorker();
        }
    }

    private void registerWorker() {
        kvClient.put(EtcdKeyPrefix.WORKER_REGISTRATION + "-" + consumerManager.getIdentifier(), Long.toString(Instant.now().getEpochSecond()));
        logger.info("Registered worker '{}'", consumerManager.getIdentifier());

    }
}
