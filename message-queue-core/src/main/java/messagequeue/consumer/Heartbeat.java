package messagequeue.consumer;

import datastorage.KVClient;
import datastorage.configuration.EtcdKeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@ConditionalOnProperty(value = "heartbeat", havingValue = "on")
public class Heartbeat {
    private final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
    private KVClient kvClient;
    private ConsumerManager consumerManager;

    public Heartbeat(KVClient kvClient, ConsumerManager consumerManager) {
        this.kvClient = kvClient;
        this.consumerManager = consumerManager;
    }

    @Scheduled(fixedDelay = 5000L)
    public void sendHeartbeat() {
        kvClient.put(
                        EtcdKeyPrefix.WORKER_HEARTBEAT + "-" + consumerManager.getIdentifier(),
                        Long.toString(Instant.now().getEpochSecond())
                )
                .thenAccept(response -> logger.trace("Sent heartbeat from '{}' at {}", consumerManager.getIdentifier(), Instant.now().getEpochSecond(), response));
    }
}
