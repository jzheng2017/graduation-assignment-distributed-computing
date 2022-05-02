package datastorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

@Service
public class RetryUtil {
    private Logger logger = LoggerFactory.getLogger(RetryUtil.class);

    public void waitUntilSuccess(Supplier<Void> supplier, long maxDurationMillis, long intervalMillis) {
        final int retryCount = (int) (maxDurationMillis / intervalMillis);
        final int duration = (int) maxDurationMillis / retryCount;
        int retries = 0;
        for (int i = 0; i < retryCount; i++) {
            try {
                supplier.get();
                logger.info("Successful attempt!");
                return;
            } catch (Exception exception) {
                try {
                    retries++;
                    logger.warn("Retry failed! Number of retries: {}. Sleeping for {} ms..", retries, duration);
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new RuntimeException(String.format("Could not successfully execute within the %d retries", retryCount));
    }
}
