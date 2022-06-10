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
     * Register a listener to listen to the specified prefix. If a value for a key changed that matches the prefix then the listener will be notified.
     * @param prefix prefix to listen for
     * @param watchListener the listener
     */
    void watchByPrefix(String prefix, WatchListener watchListener);

    /**
     * Stop watching a specific key
     * @param key key to stop listening for
     */
    void unwatch(String key);

    /**
     * Reset the {@link WatchClient} by stopping all watches and unregister them
     */
    void reset();
}
