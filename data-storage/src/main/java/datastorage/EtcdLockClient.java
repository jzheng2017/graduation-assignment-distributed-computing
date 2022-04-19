package datastorage;

import datastorage.configuration.EtcdProperties;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class EtcdLockClient implements LockClient {
    private Logger logger = LoggerFactory.getLogger(EtcdLockClient.class);
    private static final long DEFAULT_LOCK_DURATION_SECONDS = 60L;
    private Lock lockClient;
    private Lease leaseClient;

    public EtcdLockClient(EtcdProperties etcdProperties) {
        Client client = Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build();
        this.lockClient = client.getLockClient();
        this.leaseClient = client.getLeaseClient();
    }

    @Override
    public CompletableFuture<Void> lock(String name) {
        return lockClient.lock(ByteSequence.from(name.getBytes()), 0).thenAccept(c -> logger.info("Lock with the name '{}' acquired", name));
    }

    @Override
    public CompletableFuture<Void> unlock(String name) {
        return lockClient.unlock(ByteSequence.from(name.getBytes())).thenAccept(c -> logger.info("Lock with the name '{}' released", name));
    }
}
