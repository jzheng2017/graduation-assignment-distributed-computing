package datastorage;

public interface WatchClient {
    void watch(String key, WatchListener watchListener);
    void unwatch(String key);
}
