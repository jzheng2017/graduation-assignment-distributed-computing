package datastorage;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EtcdWatchClientTest {
    private EtcdWatchClient etcdWatchClient;
    @Mock
    private Watch watch;
    @Mock
    private WatchListener watchListener;
    @Mock
    private Watch.Watcher watcher;
    private Map<String, Watch.Watcher> watchers;
    private final String fakeKey = "keyzzzz";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        this.watchers = new HashMap<>();
        this.etcdWatchClient = new EtcdWatchClient(watch, watchers);
        when(watch.watch(any(), any(WatchOption.class), any(Consumer.class), any(), any())).thenReturn(watcher);
    }

    @Test
    void testThatWatchingAKeyConfiguresAndStoresCorrectly() {
        etcdWatchClient.watch(fakeKey, watchListener);

        Mockito.verify(watch).watch(eq(ByteSequence.from(fakeKey.getBytes())), any(WatchOption.class), any(Consumer.class), any(), any());
        Assertions.assertTrue(watchers.containsKey(fakeKey));
        Assertions.assertEquals(watcher, watchers.get(fakeKey));
    }

    @Test
    void testThatWatchingAKeyThatIsAlreadyBeingWatchedDoesNotCreateANewWatch() {
        watchers.put(fakeKey, watcher);
        etcdWatchClient.watch(fakeKey, watchListener);
        Mockito.verify(watch, times(0)).watch(any(), any(Consumer.class), any(), any());
    }

    @Test
    void testThatUnWatchingAKeyIsRemovedAndStoppedCorrectly() {
        watchers.put(fakeKey, watcher);
        etcdWatchClient.unwatch(fakeKey);
        verify(watcher).close();
        Assertions.assertFalse(watchers.containsKey(fakeKey));
    }
}
