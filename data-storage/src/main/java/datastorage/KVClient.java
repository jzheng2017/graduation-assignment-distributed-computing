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
     * Get the value that is bound to the key. It will return all values that has the same prefix as the provided key.
     * @param key the key to find values for
     * @return
     */
    CompletableFuture<GetResponse> get(String key);

    /**
     * Delete the value that is bound to the provided key
     * @param key the key to delete
     * @return
     */
    CompletableFuture<DeleteResponse> delete(String key);
}
