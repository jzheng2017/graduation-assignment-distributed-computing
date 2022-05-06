package worker.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.Util;
import datastorage.KVClient;
import commons.KeyPrefix;
import messagequeue.consumer.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import worker.Worker;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class EnvironmentSetup implements ApplicationRunner {
    private Logger logger = LoggerFactory.getLogger(EnvironmentSetup.class);
    private KVClient kvClient;
    private Worker worker;
    private ConsumerManager consumerManager;
    private Util util;

    public EnvironmentSetup(KVClient kvClient, Worker worker, ConsumerManager consumerManager, Util util) {
        this.kvClient = kvClient;
        this.worker = worker;
        this.consumerManager = consumerManager;
        this.util = util;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registerWorker();
        getPartitionAssignment();
        registerConsumers();
    }

    private void getPartitionAssignment() {
        try {
            kvClient.getByPrefix(KeyPrefix.PARTITION_ASSIGNMENT)
                    .thenAcceptAsync(getResponse ->
                            getResponse
                                    .keyValues()
                                    .entrySet()
                                    .stream()
                                    .filter(partitionAssignment -> partitionAssignment.getValue().equals(worker.getIdentifier()))
                                    .findFirst()
                                    .ifPresentOrElse(
                                            partitionAssignment -> worker.setAssignedPartition(
                                                    Integer.parseInt(
                                                            util.getSubstringAfterPrefix(
                                                                    KeyPrefix.PARTITION_ASSIGNMENT + "-",
                                                                    partitionAssignment.getKey()
                                                            )
                                                    )
                                            ),
                                            () -> logger.warn("Could not find any partition assigned to this worker")
                                    )
                    )
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Could not get workers partition assignment");
        }
    }

    private void registerConsumers() {
        if (worker.getAssignedPartition() >= 0) {
            try {
                final String key = KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + worker.getAssignedPartition();
                List<String> consumers = new ObjectMapper().readValue(
                        kvClient.get(key)
                                .get()
                                .keyValues()
                                .get(key),
                        List.class);
                consumers.forEach(consumerManager::registerConsumer);
            } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
                logger.error("Could not successfully start the consumers", e);
            }
        }
    }

    private void registerWorker() {
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + worker.getIdentifier(), Long.toString(Instant.now().getEpochSecond()))
                .thenAcceptAsync(ignore -> logger.info("Registered worker '{}'", worker.getIdentifier()));
    }
}
