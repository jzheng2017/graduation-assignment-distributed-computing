package kafka.consumer;

import commons.Util;
import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.dto.GetResponse;
import datastorage.dto.PutResponse;
import kafka.TestUtil;
import messagequeue.consumer.MessageProcessor;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConsumerTest {
    private kafka.consumer.KafkaConsumer kafkaConsumer;
    @Mock
    private KafkaConsumer<String, String> mockedKafkaConsumer;
    @Mock
    private TaskManager mockedTaskManager;
    @Mock
    private MessageProcessor mockedMessageProcessor;
    @Mock
    private KVClient kvClient;
    @Mock
    private LockClient lockClient;
    @Mock
    private Util util;
    private GetResponse getResponse;
    private PutResponse putResponse;
    private ConsumerRecords<String, String> consumerRecords;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        consumerRecords = new ConsumerRecords<>(Map.of(topicPartition, List.of(new ConsumerRecord<>("test", 0, 0L, "key", "value"))));
        ConsumerRecords<String, String> emptyConsumerRecords = new ConsumerRecords<>(Map.of());
        kafkaConsumer = new kafka.consumer.KafkaConsumer(mockedKafkaConsumer, "kafka", mockedTaskManager, mockedMessageProcessor, kvClient, lockClient, util);
        when(mockedKafkaConsumer.poll(any())).thenReturn(consumerRecords).thenReturn(emptyConsumerRecords);
        getResponse = new GetResponse(new HashMap<>());
        putResponse = new PutResponse("");
        when(kvClient.put(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(putResponse));
        when(kvClient.get(anyString())).thenReturn(CompletableFuture.completedFuture(getResponse));
    }


    @Test
    void testThatPollReturnsCorrectMessages() {
        Map<String, List<String>> polledMessages = kafkaConsumer.poll();

        Assertions.assertEquals(1, polledMessages.size());
        Assertions.assertTrue(polledMessages.values().stream().anyMatch(list -> list.contains("value")));
    }

    @Test
    void testThatKafkaConsumerClosesConsumerWhenStopped() throws InterruptedException {
        kafkaConsumer.start();
        TestUtil.waitUntil(() -> kafkaConsumer.isRunning(), "Kafka consumer couldn't start", 1000, 100); //give kafka consumer time to start properly
        kafkaConsumer.stop();
        TestUtil.waitUntil(() -> {
            try {
                verify(mockedKafkaConsumer).close();
                return true;
            } catch (Throwable ex) {
                return false;
            }
        }, "Kafka consumer couldn't stop", 1000, 100);
    }

    @Test
    void testThatKafkaConsumerDispatchesTasks() throws InterruptedException {
        kafkaConsumer.start();

        TestUtil.waitUntil(() -> {
            try {
                verify(mockedTaskManager, atLeastOnce()).executeTasks(anyString(), any());
                return true;
            } catch (Throwable ex) {
                return false;
            }
        }, "Kafka consumer didn't dispatch tasks", 1000, 100);
    }

    @Test
    void testThatKafkaConsumerCommitsOffsetAfterProcessingProperly() throws InterruptedException {
        kafkaConsumer.start();
        TestUtil.waitUntil(() -> kafkaConsumer.isRunning(), "Kafka consumer couldn't start", 1000, 100); //give kafka consumer time to start properly
        kafkaConsumer.stop();
        TestUtil.waitUntil(() -> {
            try {
                verify(mockedKafkaConsumer, atLeastOnce()).commitSync(any(Map.class));
                return true;
            } catch (Throwable ex) {
                return false;
            }
        }, "Kafka consumer didn't commit", 1000, 100);
    }
}
