package messagequeue.configuration;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class EnvironmentSetup {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private ConsumerManager consumerManager;
    private KVClient kvClient;


    public EnvironmentSetup(ConsumerManager consumerManager, KVClient kvClient) {
        this.consumerManager = consumerManager;
        this.kvClient = kvClient;
    }

    public void setup() {
        registerWorker();
    }

    private void registerWorker() {
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + consumerManager.getIdentifier(), Long.toString(Instant.now().getEpochSecond()))
                .thenAcceptAsync(ignore -> logger.info("Registered worker '{}'", consumerManager.getIdentifier()));
    }
}
