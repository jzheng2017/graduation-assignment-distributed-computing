package coordinator;

import coordinator.configuration.EnvironmentConfiguration;
import coordinator.partition.PartitionManager;
import datastorage.KVClient;
import datastorage.LockClient;
import commons.KeyPrefix;
import datastorage.dto.GetResponse;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public abstract class BaseIntegrationTest {
    public static final int DEFAULT_ETCD_PORT = 2379;
    public static final int NUMBER_OF_PARTITIONS = 4;
    @Autowired
    protected KVClient kvClient;
    @Autowired
    protected LockClient lockClient;
    @Autowired
    protected EnvironmentConfiguration environmentConfiguration;
    @Autowired
    protected PartitionManager partitionManager;

    @BeforeEach
    void genericSetup() throws ExecutionException, InterruptedException {
        List<String> keyPrefixesToDelete = Arrays.stream(KeyPrefix.class.getFields()).map(field -> {
            try {
                return (String)field.get(KeyPrefix.class);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        for (String keyPrefix : keyPrefixesToDelete) { //cleanup the store
            try {
                kvClient.deleteByPrefix(keyPrefix).get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
                //allow it
            }
        }
        partitionManager.createPartitions(environmentConfiguration.getPartitions());
    }
    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("partitions",() -> NUMBER_OF_PARTITIONS);
    }

    public String getValueFromGetResponse(String key, GetResponse getResponse) {
        return getResponse.keyValues().get(key);
    }
}
