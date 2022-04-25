package coordinator.partition;

import coordinator.configuration.EnvironmentConfiguration;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.KeyPrefix;
import datastorage.configuration.LockNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.when;


public class PartitionManagerIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private PartitionManager partitionManager;
    private final String workerId = "1";

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        String[] keyPrefixesToDelete = new String[]{KeyPrefix.PARTITION_ASSIGNMENT, KeyPrefix.WORKER_REGISTRATION, KeyPrefix.WORKER_HEARTBEAT, KeyPrefix.WORKER_STATISTICS};
        for (String keyPrefix : keyPrefixesToDelete) { //cleanup the store
            kvClient.deleteByPrefix(keyPrefix).get();
        }
        partitionManager.createPartitions(environmentConfiguration.getPartitions());
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
        Thread.sleep(100);
        Map<Integer, String> partitionAssignments = partitionManager.getPartitionAssignments();
        Assertions.assertFalse(partitionAssignments.containsKey(partitionNumber));
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
        Assertions.assertEquals(numberOfPartitions, partitionManager.getPartitionAssignments().size());
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
