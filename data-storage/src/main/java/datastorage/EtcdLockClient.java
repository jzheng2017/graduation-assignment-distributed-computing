package datastorage;

import datastorage.configuration.EtcdProperties;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lock.LockResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Service
public class EtcdLockClient implements LockClient {
    private Logger logger = LoggerFactory.getLogger(EtcdLockClient.class);
    private static final long DEFAULT_LOCK_DURATION_SECONDS = 60L;
    private Lock lockClient;
    private final Map<String, String> lockOwnerships = new ConcurrentHashMap<>();

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
    public void lock(String name) {
        logger.info("Trying to acquire lock '{}'", name);
        synchronized (lockOwnerships) {
            try {
               LockResponse lockResponse = lockClient.lock(ByteSequence.from(name.getBytes()), 0).get();
                logger.info("Lock '{}' acquired", name);
                lockOwnerships.put(name, lockResponse.getKey().toString());
            } catch (InterruptedException | ExecutionException e) {
                logger.warn("Lock '{}' could not be successfully acquired.",name, e);
            }
        }
    }

    @Override
    public void unlock(String name) {
        synchronized (lockOwnerships) {
            String lockOwnershipKey = lockOwnerships.get(name);
            if (lockOwnershipKey != null) {
                logger.info("Trying to release lock '{}'", name);
                try {
                    lockClient.unlock(ByteSequence.from(lockOwnershipKey.getBytes())).get();
                    logger.info("Lock '{}' released", name);
                    lockOwnerships.remove(name);
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Lock '{}' could not be successfully released.", name, e);
                }
            } else {
                logger.warn("Could not unlock '{}' as there is no active lock on it", name);
            }
        }
    }

    @Override
    public <T> T acquireLockAndExecute(String lockName, Supplier<T> supplier) {
        synchronized (lockOwnerships) {
            lock(lockName);
            T value = supplier.get();
            unlock(lockName);
            return value;
        }
    }
}
