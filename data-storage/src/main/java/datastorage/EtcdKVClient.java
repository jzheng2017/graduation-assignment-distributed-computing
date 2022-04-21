package datastorage;

import datastorage.configuration.EtcdProperties;
import datastorage.dto.DeleteResponse;
import datastorage.dto.GetResponse;
import datastorage.dto.PutResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class EtcdKVClient implements KVClient {
    private Logger logger = LoggerFactory.getLogger(EtcdKVClient.class);
    private KV kvClient;

    public EtcdKVClient(EtcdProperties etcdProperties) {
        this.kvClient= Client
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
                .thenApply(response -> new PutResponse(response.getPrevKv().getValue().toString()));
    }

    @Override
    public CompletableFuture<GetResponse> get(String key) {
        return kvClient
                .get(ByteSequence.from(key.getBytes()))
                .thenApply(response -> new GetResponse(
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
        return kvClient
                .delete(ByteSequence.from(key.getBytes()))
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
}
