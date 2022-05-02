package datastorage;

import datastorage.configuration.EtcdProperties;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class EtcdClientFactory {
    private Logger logger = LoggerFactory.getLogger(EtcdClientFactory.class);
    private EtcdProperties etcdProperties;
    private RetryUtil retryUtil;

    public EtcdClientFactory(EtcdProperties etcdProperties, RetryUtil retryUtil) {
    this.etcdProperties = etcdProperties;
        this.retryUtil = retryUtil;
    }

    public KV getKvClient() {
        Client client = getClient();
        checkForConnection(client);
        return client.getKVClient();
    }

    public Lock getLockClient() {
        Client client = getClient();
        checkForConnection(client);
        return client.getLockClient();
    }

    public Watch getWatchClient() {
        Client client = getClient();
        checkForConnection(client);
        return client.getWatchClient();
    }

    private Client getClient() {
        return Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build();
    }

    private void checkForConnection(Client client) {
        //just make a call to etcd to see if it responds
        logger.info("Checking for successful connect with etcd..");
        retryUtil.waitUntilSuccess(
                () -> {
                    try {
                        client.getClusterClient().listMember().get(1, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                },
                60000, //1 minute
                5000L); //retry every 5 secs
    }
}
