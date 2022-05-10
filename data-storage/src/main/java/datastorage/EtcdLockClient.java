package datastorage;

import io.etcd.jetcd.ByteSequence;
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
    private final Map<String, LockDetail> lockOwnerships = new ConcurrentHashMap<>();
    private RetryUtil retryUtil;

    //only for unit test purposes
    EtcdLockClient(Lock lock) {
        this.lockClient = lock;
    }

    @Autowired
    public EtcdLockClient(EtcdClientFactory etcdClientFactory) {
        this.lockClient = etcdClientFactory.getLockClient();
    }

    @PreDestroy
    void cleanup() {
        lockClient.close();
    }

    @Override
    public void lock(String name) {
        logger.info("Trying to acquire lock '{}'", name);
        try {
            LockResponse lockResponse = lockClient.lock(ByteSequence.from(name.getBytes()), 0).get();
            logger.info("Lock '{}' acquired", name);
            lockOwnerships.put(name, new LockDetail(lockResponse.getKey().toString(), Thread.currentThread().getId()));
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Lock '{}' could not be successfully acquired.", name, e);
        }
    }

    @Override
    public void unlock(String name) {
        LockDetail lockDetail = lockOwnerships.get(name);
        if (lockDetail != null) {
            if (lockDetail.threadOwnerId == Thread.currentThread().getId()) {
                logger.info("Trying to release lock '{}'", name);
                try {
                    lockClient.unlock(ByteSequence.from(lockDetail.lockOwnershipKey.getBytes())).get();
                    logger.info("Lock '{}' released", name);
                    lockOwnerships.remove(name);
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Lock '{}' could not be successfully released.", name, e);
                }
            } else {
                logger.warn("Not allowed to release lock '{}' as the current thread '{}' is not the owner of it", name, Thread.currentThread().getId());
            }
        } else {
            logger.warn("Could not unlock '{}' as there is no active lock on it", name);
        }
    }

    @Override
    public <T> T acquireLockAndExecute(String lockName, Supplier<T> supplier) {
        boolean unlocked = false;
        try {
            lock(lockName);
            T value = supplier.get();
            unlock(lockName);
            unlocked = true;
            return value;
        } finally {
            if (!unlocked) {
                unlock(lockName);
            }
        }
    }

    private record LockDetail(String lockOwnershipKey, long threadOwnerId) {
    }
}
