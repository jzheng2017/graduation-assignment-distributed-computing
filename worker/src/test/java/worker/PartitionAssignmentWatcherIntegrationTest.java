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

class PartitionAssignmentWatcherIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private GenericApplicationContext genericApplicationContext;
    @Autowired
    private Worker worker;
    @Autowired
    private ConsumerManager consumerManager;
    @Autowired
    private WatchClient watchClient;
    private PartitionAssignmentWatcher partitionAssignmentWatcher;
    private ConsumerAssignmentChangeWatcher consumerAssignmentChangeWatcher;
    private int partition = 1;
    private String consumerId = "uppercase";
    final String consumerConfiguration = "{\n" +
            " \"name\": \"uppercase\",\n" +
            " \"groupId\": \"uppercase1\",\n" +
            " \"subscriptions\": [\"input\"]\n" +
            "}";

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException, JsonProcessingException {
        genericApplicationContext.registerBean("consumerConfigurationWatcher", ConsumerConfigurationWatcher.class);
        genericApplicationContext.registerBean("partitionAssignmentWatcher", PartitionAssignmentWatcher.class);
        genericApplicationContext.registerBean("consumerAssignmentChangeWatcher", ConsumerAssignmentChangeWatcher.class);
        consumerAssignmentChangeWatcher = genericApplicationContext.getBean(ConsumerAssignmentChangeWatcher.class);
        partitionAssignmentWatcher = genericApplicationContext.getBean(PartitionAssignmentWatcher.class);
        kvClient.put(KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId, consumerConfiguration);
        kvClient.put(KeyPrefix.PARTITION_CONSUMER_ASSIGNMENT + "-" + partition, new ObjectMapper().writeValueAsString(List.of(consumerId))).get();
        kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition, worker.getIdentifier()).get();
    }

    @AfterEach
    void tearDown() {
        consumerManager.shutdown();
        watchClient.reset();
    }

    @Test
    void testThatAssigningAPartitionToWorkerGetsHandledCorrectly() {
        TestUtil.waitUntil(() -> worker.getAssignedPartition() == partition, "Partition was not updated", 1000, 100);
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().contains(consumerId), "Consumer was not started", 1000, 100);
    }

    @Test
    void testThatRemovingAPartitionFromAWorkerGetsHandledCorrectly() throws ExecutionException, InterruptedException {
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().size() > 0, "Worker is not in a correct state", 1000, 100);
        kvClient.delete(KeyPrefix.PARTITION_ASSIGNMENT + "-" + partition).get();
        TestUtil.waitUntil(() -> consumerManager.getAllConsumers().isEmpty(), "Consumers were not unregistered", 1000, 100);
    }
}
