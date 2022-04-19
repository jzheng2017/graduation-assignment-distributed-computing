package datastorage;

import datastorage.dto.DeleteResponse;
import datastorage.dto.GetResponse;
import datastorage.dto.PutResponse;

import java.util.concurrent.CompletableFuture;

public interface KVClient {
    CompletableFuture<PutResponse> put(String key, String value);

    CompletableFuture<GetResponse> get(String key);

    CompletableFuture<DeleteResponse> delete(String key);
}
