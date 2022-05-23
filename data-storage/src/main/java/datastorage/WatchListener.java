package datastorage;

import datastorage.dto.WatchResponse;

/**
 * A listener that watches a specific key in the key-value store. It allows one to perform actions if the value that is associated with the key has changed.
 */
public interface WatchListener {
    /**
     * This function will be called if the value that is associated with the key that this listener is watching has changed.
     * @param watchResponse a response containing the events that has taken place with the previous and new value.
     */
    void onNext(WatchResponse watchResponse);

    /**
     * This function will be called if watching the key has resulted in an error.
     * @param throwable the exception that was thrown
     */
    void onError(Throwable throwable);

    /**
     * This function will be called when the watcher has stopped.
     */
    void onCompleted();
}
