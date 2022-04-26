package coordinator.worker;

import coordinator.BaseIntegrationTest;
import datastorage.configuration.KeyPrefix;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

@ActiveProfiles("test-with-watchers")
public class WorkerWatcherIntegrationTest extends BaseIntegrationTest {

    @Test
    void testThatAWorkerGetsAssignedAPartition() throws ExecutionException, InterruptedException {
        final String workerId = "123";
        //ensure that there is still room for partition assignment
        Assertions.assertNotEquals(partitionManager.getNumberOfPartitions(), partitionManager.getPartitionAssignments().size());

        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + workerId, String.valueOf(Instant.now().getEpochSecond())).get();

        Thread.sleep(500);

        Assertions.assertTrue(partitionManager.getPartitionAssignments().values().stream().anyMatch(workerAssignedToPartition -> workerAssignedToPartition.equals(workerId)));
    }
}
