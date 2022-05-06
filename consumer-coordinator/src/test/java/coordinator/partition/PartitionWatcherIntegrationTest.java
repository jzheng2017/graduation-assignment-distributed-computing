package coordinator.partition;

import coordinator.BaseIntegrationTest;
import coordinator.TestUtil;
import coordinator.worker.WorkerWatcher;
import commons.KeyPrefix;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.GenericWebApplicationContext;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class PartitionWatcherIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private GenericWebApplicationContext context;
    private PartitionWatcher partitionWatcher;
    private WorkerWatcher workerWatcher;
    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        context.registerBean("partitionWatcher", PartitionWatcher.class);
        context.registerBean("workerWatcher", WorkerWatcher.class);
        partitionWatcher = (PartitionWatcher) context.getBean("partitionWatcher");
        workerWatcher = (WorkerWatcher) context.getBean("workerWatcher");

        final int numberOfPartitions = partitionManager.getNumberOfPartitions();

        for (int i = 0; i < numberOfPartitions; i++) {
            kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + i, String.valueOf(Instant.now().getEpochSecond())).get();
        }
    }

    @Test
    void testThatAChangeInPartitionCountReassignsThePartitionsOverTheWorkers() throws InterruptedException {
        final int newPartitionCount = partitionManager.getNumberOfPartitions() - 1;
        //ensure that the current partition assignments is in a correct state
        TestUtil.waitUntil(
                () -> partitionManager.getNumberOfPartitions() == partitionManager.getPartitionAssignments().size(),
                "",
                1000,
                100
        );

        //change partition count
        partitionManager.createPartitions(newPartitionCount);

        TestUtil.waitUntil(() -> newPartitionCount == partitionManager.getPartitionAssignments().size(), "", 1000, 100);
    }
}
