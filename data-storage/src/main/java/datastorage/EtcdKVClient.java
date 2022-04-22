package datastorage;

import datastorage.configuration.EtcdProperties;
import datastorage.dto.DeleteResponse;
import datastorage.dto.GetResponse;
import datastorage.dto.PutResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.netty.util.concurrent.CompleteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class EtcdKVClient implements KVClient {
    private Logger logger = LoggerFactory.getLogger(EtcdKVClient.class);
    private KV kvClient;

    //only for unit test purposes
    EtcdKVClient(KV kvClient) {
        this.kvClient = kvClient;
    }
    @Autowired
    public EtcdKVClient(EtcdProperties etcdProperties) {
        this.kvClient = Client
                .builder()
                .endpoints(etcdProperties.getBaseUrl())
                .build()
                .getKVClient();
    }

    @PreDestroy
    void cleanup() {
        this.kvClient.close();
    }

    @Override
    public CompletableFuture<PutResponse> put(String key, String value) {
        return kvClient
                .put(ByteSequence.from(key.getBytes()), ByteSequence.from(value.getBytes()))
                .thenApplyAsync(response -> new PutResponse(response.getPrevKv().getValue().toString()));
    }

    @Override
    public CompletableFuture<GetResponse> get(String key) {
        return getWithOptions(key, GetOption.DEFAULT);
    }

    @Override
    public CompletableFuture<GetResponse> getByPrefix(String prefix) {
        return getWithOptions(prefix, GetOption.newBuilder().isPrefix(true).build());
    }

    private CompletableFuture<GetResponse> getWithOptions(String keyOrPrefix, GetOption getOption) {
        return kvClient
                .get(ByteSequence.from(keyOrPrefix.getBytes()), getOption)
                .thenApplyAsync(response -> new GetResponse(
                                response.getKvs()
                                        .stream()
                                        .collect(
                                                Collectors.toMap(
                                                        k -> k.getKey().toString(),
                                                        k -> k.getValue().toString())
                                        )
                        )
                );
    }

    @Override
    public CompletableFuture<DeleteResponse> delete(String key) {
        return deleteWithOptions(key, DeleteOption.DEFAULT);
    }

    @Override
    public CompletableFuture<DeleteResponse> deleteByPrefix(String prefix) {
        return deleteWithOptions(
                prefix,
                DeleteOption
                        .newBuilder()
                        .isPrefix(true)
                        .build()
        );
    }

    private CompletableFuture<DeleteResponse> deleteWithOptions(String keyOrPrefix, DeleteOption deleteOption) {
        return kvClient
                .delete(ByteSequence.from(keyOrPrefix.getBytes()), deleteOption)
                .thenApply(response -> new DeleteResponse(
                                response.getPrevKvs()
                                        .stream()
                                        .collect(
                                                Collectors.toMap(
                                                        k -> k.getKey().toString(),
                                                        k -> k.getValue().toString()
                                                )
                                        )
                        )
                );
    }

    @Override
    public boolean keyExists(String key) {
        try {
            return get(key).thenApply(getResponse -> !getResponse.keyValues().isEmpty()).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not successfully check for key '{}' existence", key);
            return false;
        }
    }
}
