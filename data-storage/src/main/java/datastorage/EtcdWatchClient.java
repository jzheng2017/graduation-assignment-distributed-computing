package datastorage;

import datastorage.configuration.EtcdProperties;
import datastorage.dto.WatchEvent;
import datastorage.dto.WatchResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class EtcdWatchClient implements WatchClient {
    private Logger logger = LoggerFactory.getLogger(EtcdWatchClient.class);
    private Watch watchClient;
    private Map<String, Watch.Watcher> watchers = new HashMap<>();

    public EtcdWatchClient(EtcdProperties etcdProperties) {
        Client client = Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build();
        this.watchClient = client.getWatchClient();
    }

    @Override
    public void watch(String key, WatchListener watchListener) {
        watchClient.watch(
                ByteSequence.from(key.getBytes()),
                watchResponse -> watchListener.onNext(new WatchResponse(mapWatchEvents(watchResponse.getEvents()))),
                watchListener::onError,
                watchListener::onCompleted
        );
        logger.info("Registered listener to watch key '{}'", key);
    }

    @Override
    public void unwatch(String key) {
        if (watchers.containsKey(key)) {
            watchers.get(key).close();
            watchers.remove(key);
            logger.info("Unwatched key '{}'", key);
        } else {
            logger.warn("Can not unwatch key '{}' as no active watch has been registered on that key", key);
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
                ))
                .collect(Collectors.toList());
    }
}
