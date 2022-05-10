package worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.KeyPrefix;
import datastorage.WatchClient;
import messagequeue.consumer.ConsumerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.GenericApplicationContext;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class ConsumerAssignmentChangeWatcherIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private GenericApplicationContext genericApplicationContext;
    @Autowired
    private Worker worker;
    @Autowired
    private ConsumerManager consumerManager;
    @Autowired
    private WatchClient watchClient;
    private ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher;
    private int partition = 1;
    private String consumerId = "uppercase";
    final String consumerConfiguration = "{\n" +
            " \"name\": \"uppercase\",\n" +
            " \"groupId\": \"uppercase1\",\n" +
            " \"subscriptions\": [\"input\"]\n" +
            "}";

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        genericApplicationContext.registerBean("partitionAssignmentWatcher", PartitionAssignmentWatcher.class);
        genericApplicationContext.registerBean("consumerAssignmentChangeWatcher", ConsumerAssignmentChangeWatcher.class);
        consumerAssignmentChangeWatcher = genericApplicationContext.getBean(ConsumerAssignmentChangeWatcher.class);
        kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition, worker.getIdentifier()).get();
        worker.setAssignedPartition(partition);
        consumerAssignmentChangeWatcher.partitionChanged(partition);
    }

    @AfterEach
    void tearDown() {
        consumerManager.shutdown();
        watchClient.reset();
    }

    @Test
    void testThatAddingAConsumerToThePartitionOfTheWorkerGetsStarted() throws JsonProcessingException, ExecutionException, InterruptedException {
        kvClient.put(KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId, consumerConfiguration).get();
        kvClient.put(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition, new ObjectMapper().writeValueAsString(List.of(consumerId))).get();
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().contains(consumerId), "Consumer was not added", 1000, 100);
    }

    @Test
    void testThatRemovingAConsumerFromThePartitionOfTheWorkerGetsStopped() throws ExecutionException, JsonProcessingException, InterruptedException {
        kvClient.put(KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId, consumerConfiguration).get();
        kvClient.put(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition, new ObjectMapper().writeValueAsString(List.of(consumerId))).get();
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().contains(consumerId), "Consumer was not added", 1000, 100);

        kvClient.put(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition, new ObjectMapper().writeValueAsString(List.of())).get();
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().isEmpty(), "Consumer was removed", 1000, 100);
    }
}
