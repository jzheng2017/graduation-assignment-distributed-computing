package datastorage;

import datastorage.configuration.EtcdProperties;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EtcdWatchClient implements WatchClient {
    private Logger logger = LoggerFactory.getLogger(EtcdWatchClient.class);
    private Watch watchClient;
    private Map<String, Watch.Watcher> watchers = new ConcurrentHashMap<>();

    //only for unit test purposes
    EtcdWatchClient(Watch watchClient, Map<String, Watch.Watcher> watchers) {
        this.watchClient = watchClient;
        this.watchers = watchers;
    }

    @Autowired
    public EtcdWatchClient(EtcdProperties etcdProperties) {
        Client client = Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build();
        this.watchClient = client.getWatchClient();
    }

    @PreDestroy
    void cleanup() {
        this.watchClient.close();
    }

    @Override
    public void watch(String key, WatchListener watchListener) {
        synchronized (watchers) {
            if (!watchers.containsKey(key)) {
                Watch.Watcher watcher = watchClient.watch(
                        ByteSequence.from(key.getBytes()),
                        watchResponse -> watchListener.onNext(new WatchResponse(mapWatchEvents(watchResponse.getEvents()))),
                        watchListener::onError,
                        watchListener::onCompleted
                );
                watchers.put(key, watcher);
                logger.info("Registered listener to watch key '{}'", key);
            } else {
                logger.warn("Can not add new watcher to listen to key '{}' because there is already an existing watch listening to it", key);
            }
        }
    }

    @Override
    public void unwatch(String key) {
        synchronized (watchers) {
            if (watchers.containsKey(key)) {
                watchers.get(key).close();
                watchers.remove(key);
                logger.info("Unwatched key '{}'", key);
            } else {
                logger.warn("Can not unwatch key '{}' as no active watch has been registered on that key", key);
            }
        }
    }

    private List<WatchEvent> mapWatchEvents(List<io.etcd.jetcd.watch.WatchEvent> events) {
        return events
                .stream()
                .map(event -> new WatchEvent(
                        event.getKeyValue().getKey().toString(),
                        event.getKeyValue().getValue().toString(),
                        event.getPrevKV().getKey().toString(),
                        event.getPrevKV().getValue().toString()
                )).toList();
    }
}
