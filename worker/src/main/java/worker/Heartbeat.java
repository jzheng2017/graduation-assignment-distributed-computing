package worker;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import worker.Worker;

import java.time.Instant;

/**
 * This class is responsible for sending a periodic heartbeat. This is necessary so that the coordinator knows that the worker is still alive.
 */
@Service
public class Heartbeat {
    private final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
    private KVClient kvClient;
    private Worker worker;

    public Heartbeat(KVClient kvClient, Worker worker) {
        this.kvClient = kvClient;
        this.worker = worker;
    }

    @Scheduled(fixedDelay = 5000L)
    public void sendHeartbeat() {
        kvClient.put(
                        KeyPrefix.WORKER_HEARTBEAT + "-" + worker.getIdentifier(),
                        Long.toString(Instant.now().getEpochSecond())
                )
                .thenAcceptAsync(response -> logger.trace("Sent heartbeat from '{}' at {}", worker.getIdentifier(), Instant.now().getEpochSecond(), response));
    }
}
