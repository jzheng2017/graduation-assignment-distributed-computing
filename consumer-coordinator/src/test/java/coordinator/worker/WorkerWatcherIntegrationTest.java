package coordinator.worker;

import commons.KeyPrefix;
import coordinator.BaseIntegrationTest;
import coordinator.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.GenericWebApplicationContext;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class WorkerWatcherIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private GenericWebApplicationContext context;
    private WorkerWatcher workerWatcher;

    @BeforeEach
    void setup() {
        context.registerBean("workerWatcher", WorkerWatcher.class);
        workerWatcher = (WorkerWatcher) context.getBean("workerWatcher");
    }

    @Test
    void testThatAWorkerGetsAssignedAPartition() throws ExecutionException, InterruptedException {
        final String workerId = "blabla";
        //ensure that there is still room for partition assignment
        Assertions.assertNotEquals(partitionManager.getNumberOfPartitions(), partitionManager.getPartitionAssignments().size());

        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + workerId, String.valueOf(Instant.now().getEpochSecond())).get();


        TestUtil.waitUntil(
                () -> partitionManager.getPartitionAssignments().values().stream().anyMatch(workerAssignedToPartition -> workerAssignedToPartition.equals(workerId)),
                "",
                1000,
                100);
    }
}
