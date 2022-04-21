package datastorage;

/**
 * This client allows one to watch certain keys and execute logic when the value that is bound to the key has changed.
 */
public interface WatchClient {
    /**
     * Register a listener to listen to the specified key. If the value for the key changed then the listener will be notified.
     * @param key key to listen for
     * @param watchListener the listener
     */
    void watch(String key, WatchListener watchListener);

    /**
     * Stop watching a specific key
     * @param key key to stop listening for
     */
    void unwatch(String key);
}
