package messagequeue.consumer.builder;

import commons.KeyPrefix;
import datastorage.KVClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * A configuration store that allows one to retrieve consumer configurations
 */
@Service
public class ConsumerConfigurationReader {
    private Logger logger = LoggerFactory.getLogger(ConsumerConfigurationReader.class);
    private KVClient kvClient;

    public ConsumerConfigurationReader(KVClient kvClient) {
        this.kvClient = kvClient;
    }

    public String getConsumerConfiguration(String consumerId) {
        final String key = KeyPrefix.CONSUMER_CONFIGURATION + "-" + consumerId;
        try {
            return kvClient.get(key).thenApply(getResponse -> getResponse.keyValues().get(key)).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            logger.error("Could not retrieve consumer configuration of '{}'", consumerId, e);
            return null;
        }
    }
}
