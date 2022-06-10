package messagequeue.consumer;

import java.util.function.BooleanSupplier;

public class TestUtil {
    public static void waitUntil(BooleanSupplier supplier, String messageOnTimeout, long timeout, int interval) {
        if (timeout < interval) {
            throw new IllegalArgumentException(
                    String.format("Invalid timeout. Timeout must be greater than %d and divisible by %d.", interval, interval));
        }
        int steps = (int) timeout / interval;
        for (int i = 0; i < steps; i++) {
            try {
                if (supplier.getAsBoolean())
                    return;
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
        throw new RuntimeException(messageOnTimeout + " (timeout=" + timeout +")");
    }
}
