package datastorage;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class EtcdKVClientTest {
    @InjectMocks
    private EtcdKVClient etcdKVClient;
    @Mock
    private KV kvClient;
    @Mock
    private PutResponse putResponse;
    @Mock
    private GetResponse getResponse;
    @Mock
    private DeleteResponse deleteResponse;
    @Mock
    private KeyValue keyValue;
    @Captor
    private ArgumentCaptor<GetOption> getOptionArgumentCaptor;
    @Captor
    private ArgumentCaptor<DeleteOption> deleteOptionArgumentCaptor;
    private final String fakeKey = "beep";
    private final String fakeValue = "boop";
    private final String fakePrevValue = "fake boop";
    private List<KeyValue> listKeyValue;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        this.etcdKVClient = new EtcdKVClient(kvClient);
        this.listKeyValue = List.of(keyValue);
        when(kvClient.put(any(), any())).thenReturn(CompletableFuture.supplyAsync(() -> putResponse));
        when(kvClient.get(any(), any())).thenReturn(CompletableFuture.supplyAsync(() -> getResponse));
        when(kvClient.delete(any(), any())).thenReturn(CompletableFuture.supplyAsync(() -> deleteResponse));

        when(putResponse.getPrevKv()).thenReturn(keyValue);
        when(getResponse.getKvs()).thenReturn(listKeyValue);
        when(deleteResponse.getPrevKvs()).thenReturn(listKeyValue);

        when(keyValue.getValue()).thenReturn(ByteSequence.from(fakePrevValue.getBytes()));
        when(keyValue.getKey()).thenReturn(ByteSequence.from(fakeKey.getBytes()));
    }

    @Test
    void testThatPuttingKeyAndValueIsDoneCorrectly() {
        etcdKVClient.put(fakeKey, fakeValue);

        Mockito.verify(kvClient).put(ByteSequence.from(fakeKey.getBytes()), ByteSequence.from(fakeValue.getBytes()));
    }

    @Test
    void testThatPuttingKeyAndValueMapsPutResponseCorrectly() throws ExecutionException, InterruptedException {
        datastorage.dto.PutResponse putResponse = etcdKVClient.put(fakeKey, fakeValue).get();

        Assertions.assertEquals(fakePrevValue, putResponse.prevValue());
    }

    @Test
    void testThatGettingKeyAndValueWithoutRangeConfiguresCorrectly() {
        etcdKVClient.get(fakeKey);

        Mockito.verify(kvClient).get(eq(ByteSequence.from(fakeKey.getBytes())), getOptionArgumentCaptor.capture());
        GetOption capturedGetOption = getOptionArgumentCaptor.getValue();

        Assertions.assertFalse(capturedGetOption.isPrefix());
    }

    @Test
    void testThatGettingKeyAndValueWithRangeConfiguresCorrectly() {
        etcdKVClient.getByPrefix(fakeKey);

        Mockito.verify(kvClient).get(eq(ByteSequence.from(fakeKey.getBytes())), getOptionArgumentCaptor.capture());
        GetOption capturedGetOption = getOptionArgumentCaptor.getValue();
        Assertions.assertTrue(capturedGetOption.isPrefix());
    }

    @Test
    void testThatGettingKeyAndValueMapsResultCorrectly() throws ExecutionException, InterruptedException {
        datastorage.dto.GetResponse getResponse = etcdKVClient.get(fakeKey).get();
        datastorage.dto.GetResponse prefixGetResponse = etcdKVClient.getByPrefix(fakeKey).get();

        Map<String, String> expectedMap = listKeyValue.stream()
                .collect(
                        Collectors.toMap(
                                k -> k.getKey().toString(),
                                k -> k.getValue().toString())
                );

        Assertions.assertEquals(expectedMap, getResponse.keyValues());
        Assertions.assertEquals(expectedMap, prefixGetResponse.keyValues());
    }

    @Test
    void testThatDeletingKeyAndValueWithoutRangeConfiguresCorrectly() {
        etcdKVClient.delete(fakeKey);

        Mockito.verify(kvClient).delete(eq(ByteSequence.from(fakeKey.getBytes())), deleteOptionArgumentCaptor.capture());
        DeleteOption capturedDeleteOption = deleteOptionArgumentCaptor.getValue();

        Assertions.assertFalse(capturedDeleteOption.isPrefix());
    }

    @Test
    void testThatDeletingKeyAndValueWithRangeConfiguresCorrectly() {
        etcdKVClient.deleteByPrefix(fakeKey);

        Mockito.verify(kvClient).delete(eq(ByteSequence.from(fakeKey.getBytes())), deleteOptionArgumentCaptor.capture());
        DeleteOption capturedDeleteOption = deleteOptionArgumentCaptor.getValue();
        Assertions.assertTrue(capturedDeleteOption.isPrefix());
    }

    @Test
    void testThatDeletingKeyAndValueMapsResultCorrectly() throws ExecutionException, InterruptedException {
        datastorage.dto.DeleteResponse deleteResponse = etcdKVClient.delete(fakeKey).get();
        datastorage.dto.DeleteResponse prefixDeleteResponse = etcdKVClient.deleteByPrefix(fakeKey).get();

        Map<String, String> expectedMap = listKeyValue.stream()
                .collect(
                        Collectors.toMap(
                                k -> k.getKey().toString(),
                                k -> k.getValue().toString())
                );

        Assertions.assertEquals(expectedMap, deleteResponse.prevKeyValues());
        Assertions.assertEquals(expectedMap, prefixDeleteResponse.prevKeyValues());
    }
}
