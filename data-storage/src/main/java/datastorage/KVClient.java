package datastorage;

import datastorage.dto.DeleteResponse;
import datastorage.dto.GetResponse;
import datastorage.dto.PutResponse;

import java.util.concurrent.CompletableFuture;

public interface KVClient {
    /**
     * Create or update the value that is bound to the key
     * @param key the key the value will be bound to
     * @param value the value to save
     * @return
     */
    CompletableFuture<PutResponse> put(String key, String value);

    /**
     * Get the value that is bound to the key.
     * @param key the key to find values for
     * @return
     */
    CompletableFuture<GetResponse> get(String key);

    /**
     * Get all values for which the key matches the provided prefix.
     * @param prefix the prefix to match to
     * @return
     */
    CompletableFuture<GetResponse> getByPrefix(String prefix);


    /**
     * Delete the value that is bound to the provided key
     * @param key the key to delete
     * @return
     */
    CompletableFuture<DeleteResponse> delete(String key);

    /**
     * Delete all keys that matches the provided prefix
     * @param prefix the prefix
     * @return
     */
    CompletableFuture<DeleteResponse> deleteByPrefix(String prefix);

    /**
     * Determines whether the passed in key exists in the store
     * @param key the key
     * @return true if it exists, false otherwise
     */
    boolean keyExists(String key);
}
