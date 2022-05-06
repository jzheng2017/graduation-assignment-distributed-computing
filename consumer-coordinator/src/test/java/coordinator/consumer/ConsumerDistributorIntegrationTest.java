package coordinator.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import commons.ConsumerProperties;
import coordinator.BaseIntegrationTest;
import coordinator.ConsumerCoordinator;
import coordinator.ConsumerStatus;
import coordinator.TestUtil;
import commons.WorkerStatistics;
import commons.KeyPrefix;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.GenericWebApplicationContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ConsumerDistributorIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private GenericWebApplicationContext context;
    @Autowired
    private ConsumerCoordinator consumerCoordinator;
    private ConsumerDistributor consumerDistributor;
    private final String workerId = "1";
    private final String workerId2 = "2";
    private final String consumerId = "uppercase";

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException, JsonProcessingException {
        context.registerBean("consumerDistributor", ConsumerDistributor.class);
        consumerDistributor = (ConsumerDistributor) context.getBean("consumerDistributor");
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + workerId, String.valueOf(Instant.now().getEpochSecond())).get();
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + workerId2, String.valueOf(Instant.now().getEpochSecond())).get();
        kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-0", workerId).get();
        kvClient.put(KeyPrefix.PARTITION_ASSIGNMENT + "-1", workerId2).get();
        kvClient.put(KeyPrefix.WORKER_STATISTICS + "-1", new ObjectMapper().writeValueAsString(new WorkerStatistics("1", 0, 0, 0,  new ArrayList<>(), new ArrayList<>(), Instant.now().getEpochSecond())));
        kvClient.put(KeyPrefix.WORKER_STATISTICS + "-1", new ObjectMapper().writeValueAsString(new WorkerStatistics("2", 0, 0, 0,  new ArrayList<>(), new ArrayList<>(), Instant.now().getEpochSecond())));
    }

    @Test
    void testThatANewlyAddedConsumerGetsPlacedOnAPartition() {
        TestUtil.waitUntil(() -> partitionManager.getPartitionAssignments().size() == 2, "Incorrect number of partition assignments.", 1000, 100);

        consumerCoordinator.addConsumerConfiguration(new ConsumerProperties(consumerId, "uppercase1", Set.of("input")));

        TestUtil.waitUntil(() -> consumerCoordinator.getConsumerStatus(consumerId).equals(ConsumerStatus.ASSIGNED), "Consumer was not assigned to any partition", 1000, 100);
    }

    @Test
    void testThatAConsumerThatGetsRemovedIsAlsoUnassigned() {
        testThatANewlyAddedConsumerGetsPlacedOnAPartition();
        consumerCoordinator.removeConsumerConfiguration(consumerId);
        TestUtil.waitUntil(() -> consumerCoordinator.getConsumerStatus(consumerId).equals(ConsumerStatus.UNASSIGNED), "Consumer was not successfully unassigned", 1000, 100);
    }
}
