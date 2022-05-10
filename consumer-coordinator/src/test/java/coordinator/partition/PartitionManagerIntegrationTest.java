package coordinator.partition;

import commons.KeyPrefix;
import coordinator.BaseIntegrationTest;
import coordinator.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;


class PartitionManagerIntegrationTest extends BaseIntegrationTest {
    private final String workerId = "1";

    @BeforeEach
    void setup() {
        kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + workerId, String.valueOf(Instant.now().getEpochSecond()));
    }

    @Test
    void testThatTheNumberOfPartitionsMatchesWhatIsConfigured() {
        final int expectedNumberOfPartitions = environmentConfiguration.getPartitions();
        final int actualNumberOfPartitions = partitionManager.getNumberOfPartitions();

        Assertions.assertEquals(expectedNumberOfPartitions, actualNumberOfPartitions);
    }

    @Test
    void testThatAssigningAPartitionToAWorkerWorksCorrectly() throws InterruptedException {
        final int partitionNumber = 0;
        partitionManager.assignPartition(partitionNumber, workerId);

        Map<Integer, String> partitionAssignments = partitionManager.getPartitionAssignments();
        Assertions.assertTrue(partitionAssignments.containsKey(partitionNumber));
        Assertions.assertEquals(workerId, partitionAssignments.get(partitionNumber));
    }

    @Test
    void testThatAssigningAPartitionToANonExistingWorkerDoesNotWork() throws InterruptedException {
        final int partitionNumber = 0;
        partitionManager.assignPartition(partitionNumber, "I do not exist!");

        TestUtil.waitUntil(() -> !partitionManager.getPartitionAssignments().containsKey(partitionNumber), "", 500, 100);
    }

    @Test
    void testThatAssigningAPartitionNumberThatExceedsTheNumberOfAvailablePartitionsDoesNotWork() {
        final int partitionNumber = environmentConfiguration.getPartitions(); //partitions are 0 indexed, so if getPartitions for instance returns 4 then only partitions {0, 1, 2, 3} exists and 4 should be out of range.
        partitionManager.assignPartition(partitionNumber, workerId);

        Map<Integer, String> partitionAssignments = partitionManager.getPartitionAssignments();
        Assertions.assertFalse(partitionAssignments.containsKey(partitionNumber));
    }

    @Test
    void testThatAssigningMultiplePartitionsToASingleWorkerIsNotAllowed() {
        partitionManager.assignPartition(0, workerId);
        partitionManager.assignPartition(1, workerId);

        Map<Integer, String> partitionAssignments = partitionManager.getPartitionAssignments();
        Assertions.assertTrue(partitionAssignments.containsKey(0));
        Assertions.assertFalse(partitionAssignments.containsKey(1));
    }

    @Test
    void testThatRemovingAPartitionAssignmentFromAWorkerWorksCorrectly() {
        final int partitionNumber = 0;
        partitionManager.assignPartition(partitionNumber, workerId);

        //ensure that partition has been assigned to the worker
        Assertions.assertTrue(partitionManager.getPartitionAssignments().containsKey(partitionNumber));
        Assertions.assertEquals(workerId, partitionManager.getPartitionAssignments().get(partitionNumber));

        partitionManager.removePartitionAssignment(partitionNumber);
        Assertions.assertFalse(partitionManager.getPartitionAssignments().containsKey(partitionNumber));
    }

    @Test
    void testThatPartitionAssignmentToAllAvailableWorkersWorksCorrectly() throws ExecutionException, InterruptedException {
        final int numberOfPartitions = partitionManager.getNumberOfPartitions();

        for (int i = 0; i < numberOfPartitions; i++) {
            kvClient.put(KeyPrefix.WORKER_REGISTRATION + "-" + i, String.valueOf(Instant.now().getEpochSecond())).get();
        }

        partitionManager.resetPartitionAssignmentsAndReassign();

        //the partition numbers in the map must be unique, so if the size of the map equals the number of partitions, then all partitions have been distributed
        TestUtil.waitUntil(() -> numberOfPartitions == partitionManager.getPartitionAssignments().size(), "Partitions were not assigned correctly", 1000, 100);
    }

    @Test
    void testThatCreatingNewPartitionsWorksCorrectly() {
        final int newPartitionCount = 6;
        Assertions.assertEquals(environmentConfiguration.getPartitions(), partitionManager.getNumberOfPartitions());
        partitionManager.createPartitions(newPartitionCount);
        Assertions.assertEquals(newPartitionCount, partitionManager.getNumberOfPartitions());
    }

    @Test
    void testThatChangingPartitionCountToZeroOrNegativeIsNotAllowed() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> partitionManager.createPartitions(0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> partitionManager.createPartitions(-1));
    }
}
