package datastorage;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EtcdLockClientTest {
    private EtcdLockClient etcdLockClient;
    @Mock
    private Lock lock;
    @Mock
    private LockResponse lockResponse;
    @Mock
    private UnlockResponse unlockResponse;
    private final String lockName = "A very special lock";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        this.etcdLockClient = new EtcdLockClient(lock);
        when(lock.lock(any(), anyLong())).thenReturn(CompletableFuture.supplyAsync(() -> lockResponse));
        when(lock.unlock(any())).thenReturn(CompletableFuture.supplyAsync(() -> unlockResponse));
    }

    @Test
    void testThatLockingConfiguresTheLockCorrectly() {
        etcdLockClient.lock(lockName);
        Mockito.verify(lock).lock(ByteSequence.from(lockName.getBytes()), 0);
    }

    @Test
    void testThatUnlockingConfiguresTheRequestCorrectly() {
        etcdLockClient.unlock(lockName);
        Mockito.verify(lock).unlock(ByteSequence.from(lockName.getBytes()));
    }

    @Test
    void testThatAcquiringLockAndExecutingAcquiresTheLockAndThenExecutesAndFinallyReleasesTheLock() {
        final ExecuteMe executeMe = new ExecuteMe();

        etcdLockClient.acquireLockAndExecute(lockName, () -> {
            executeMe.execute();
            return null;
        });

        Mockito.verify(lock).lock(ByteSequence.from(lockName.getBytes()), 0);
        Assertions.assertTrue(executeMe.executed);
        Mockito.verify(lock).unlock(ByteSequence.from(lockName.getBytes()));
    }

    private class ExecuteMe {
        private boolean executed = false;

        public void execute() {
            executed = true;
        }
    }
}
