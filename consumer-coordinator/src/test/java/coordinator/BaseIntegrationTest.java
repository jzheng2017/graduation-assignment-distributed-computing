package coordinator;

import coordinator.configuration.EnvironmentConfiguration;
import coordinator.partition.PartitionManager;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.EtcdProperties;
import datastorage.configuration.KeyPrefix;
import datastorage.dto.GetResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;

import javax.annotation.PostConstruct;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(classes = {BaseIntegrationTest.TestConfig.class})
public abstract class BaseIntegrationTest {
    public static final int DEFAULT_ETCD_PORT = 2379;
    public static final int NUMBER_OF_PARTITIONS = 4;
    public static GenericContainer etcd;
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
        when(environmentConfiguration.getPartitions()).thenReturn(NUMBER_OF_PARTITIONS);
        String[] keyPrefixesToDelete = new String[]{KeyPrefix.PARTITION_ASSIGNMENT, KeyPrefix.WORKER_REGISTRATION, KeyPrefix.WORKER_HEARTBEAT, KeyPrefix.WORKER_STATISTICS};
        for (String keyPrefix : keyPrefixesToDelete) { //cleanup the store
            kvClient.deleteByPrefix(keyPrefix).get();
        }
        partitionManager.createPartitions(environmentConfiguration.getPartitions());
    }

    @BeforeAll
    static void beforeAll() {
//        etcd = new GenericContainer(DockerImageName.parse("bitnami/etcd:latest")).withExposedPorts(DEFAULT_ETCD_PORT).withEnv("ALLOW_NONE_AUTHENTICATION", "yes");
//        etcd.start();
    }

    @AfterAll
    static void afterAll() {
//        etcd.close();
    }

    @TestConfiguration
    @Profile("test")
    public static class TestConfig {
        @MockBean
        private EtcdProperties etcdProperties;
        @MockBean
        private EnvironmentConfiguration environmentConfiguration;
        @PostConstruct
        public void setup() {
//            when(etcdProperties.getBaseUrl()).thenReturn("http://localhost:" + etcd.getMappedPort(DEFAULT_ETCD_PORT));
            when(etcdProperties.getBaseUrl()).thenReturn("http://localhost:2379");
            when(environmentConfiguration.getPartitions()).thenReturn(NUMBER_OF_PARTITIONS);
        }
    }

    public String getValueFromGetResponse(String key, GetResponse getResponse) {
        return getResponse.keyValues().get(key);
    }
}