package datastorage;

import datastorage.configuration.EtcdProperties;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Service
public class EtcdLockClient implements LockClient {
    private Logger logger = LoggerFactory.getLogger(EtcdLockClient.class);
    private static final long DEFAULT_LOCK_DURATION_SECONDS = 60L;
    private Lock lockClient;

    //only for unit test purposes
    EtcdLockClient(Lock lock) {
        this.lockClient = lock;
    }

    @Autowired
    public EtcdLockClient(EtcdProperties etcdProperties) {
        Client client = Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build();
        this.lockClient = client.getLockClient();
    }

    @PreDestroy
    void cleanup() {
        lockClient.close();
    }

    @Override
    public CompletableFuture<Void> lock(String name) {
        return lockClient.lock(ByteSequence.from(name.getBytes()), 0).thenAcceptAsync(c -> logger.info("Lock '{}' acquired", name));
    }

    @Override
    public CompletableFuture<Void> unlock(String name) {
        return lockClient.unlock(ByteSequence.from(name.getBytes())).thenAcceptAsync(c -> logger.info("Lock '{}' released", name));
    }

    @Override
    public <T> T acquireLockAndExecute(String lockName, Supplier<T> supplier) {
        try {
            return lock(lockName).thenApplyAsync(ignore -> supplier.get()).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.info("Could not successfully acquire lock '{}' or execute the supplier", lockName, e);
            return null;
        } finally {
            unlock(lockName);
        }
    }
}
