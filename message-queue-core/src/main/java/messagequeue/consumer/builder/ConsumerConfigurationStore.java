package messagequeue.consumer.builder;

import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class ConsumerConfigurationStore {
    private Logger logger = LoggerFactory.getLogger(ConsumerConfigurationStore.class);
    private KVClient kvClient;

    public ConsumerConfigurationStore(KVClient kvClient) {
        this.kvClient = kvClient;
    }

    public String getConsumerConfiguration(String consumerId) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        try {
            return kvClient.get(key).thenApply(getResponse -> getResponse.keyValues().get(key)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Could not retrieve consumer configuration of '{}'", consumerId, e);
            return null;
        }
    }
}
