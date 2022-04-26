package worker.configuration;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import worker.Worker;

import java.time.Instant;

@Component
public class EnvironmentSetup implements ApplicationRunner {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private KVClient kvClient;
    private Worker worker;

    public EnvironmentSetup(KVClient kvClient, Worker worker) {
        this.kvClient = kvClient;
        this.worker = worker;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registerWorker();
    }

    private void registerWorker() {
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + worker.getIdentifier(), Long.toString(Instant.now().getEpochSecond()))
                .thenAcceptAsync(ignore -> logger.info("Registered worker '{}'", worker.getIdentifier()));
    }
}
