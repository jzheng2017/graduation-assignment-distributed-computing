package coordinator.partition;

import coordinator.BaseIntegrationTest;
import datastorage.configuration.KeyPrefix;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

@ActiveProfiles("test-with-watchers")
public class PartitionWatcherIntegrationTest extends BaseIntegrationTest {

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        final int numberOfPartitions = partitionManager.getNumberOfPartitions();

        for (int i = 0; i < numberOfPartitions; i++) {
            kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + i, String.valueOf(Instant.now().getEpochSecond())).get();
        }
    }

    @Test
    void testThatAChangeInPartitionCountReassignsThePartitionsOverTheWorkers() throws InterruptedException {
        Thread.sleep(1000); //give the watcher some time to assign partitions to newly added workers
        final int newPartitionCount = partitionManager.getNumberOfPartitions() - 1;
        //ensure that the current partition assignments is in a correct state
        Assertions.assertEquals(partitionManager.getNumberOfPartitions(), partitionManager.getPartitionAssignments().size());

        //change partition count
        partitionManager.createPartitions(newPartitionCount);

        Thread.sleep(500);
        Assertions.assertEquals(newPartitionCount, partitionManager.getPartitionAssignments().size());
    }
}
