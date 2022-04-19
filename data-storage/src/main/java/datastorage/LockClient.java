package datastorage;

import datastorage.dto.LockResponse;

import java.util.concurrent.CompletableFuture;

public interface LockClient {
    CompletableFuture<Void> lock(String name);

    CompletableFuture<Void> unlock(String name);
}
