package coordinator.partition;

import datastorage.configuration.EtcdProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.PostConstruct;

import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(classes = {PartitionManagerIntegrationTest.TestConfig.class})
public class PartitionManagerIntegrationTest {
    public static final int DEFAULT_ETCD_PORT = 2379;
    @Autowired
    private PartitionManager partitionManager;
    public static GenericContainer etcd;

    @BeforeAll
    static void beforeAll() {
        etcd = new GenericContainer(DockerImageName.parse("bitnami/etcd:latest")).withExposedPorts(DEFAULT_ETCD_PORT).withEnv("ALLOW_NONE_AUTHENTICATION", "yes");
        etcd.start();
    }

    @AfterAll
    static void afterAll() {
        etcd.close();
    }



    @TestConfiguration
    @Profile("test")
    public static class TestConfig {
        @MockBean
        private EtcdProperties etcdProperties;
        @PostConstruct
        public void setup() {
            when(etcdProperties.getBaseUrl()).thenReturn("http://localhost:" + etcd.getMappedPort(DEFAULT_ETCD_PORT));
        }
    }
}
