package kafka.consumer;

import datastorage.KVClient;
import datastorage.LockClient;
import messagequeue.Util;
import messagequeue.consumer.MessageProcessor;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
    private ConsumerRecords<String, String> consumerRecords;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        consumerRecords = new ConsumerRecords<>(Map.of(topicPartition, List.of(new ConsumerRecord<>("test", 0, 0L, "key", "value"))));
        ConsumerRecords<String, String> emptyConsumerRecords = new ConsumerRecords<>(Map.of());
        kafkaConsumer = new kafka.consumer.KafkaConsumer(mockedKafkaConsumer, "kafka", mockedTaskManager, mockedMessageProcessor, kvClient, lockClient, util);
        when(mockedKafkaConsumer.poll(any())).thenReturn(consumerRecords).thenReturn(emptyConsumerRecords);
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
        Thread.sleep(10); //give kafka consumer time to stop properly
        kafkaConsumer.stop();
        Thread.sleep(10); //give kafka consumer time to stop properly
        verify(mockedKafkaConsumer).close();
    }

    @Test
    void testThatKafkaConsumerDispatchesTasks() throws InterruptedException {
        kafkaConsumer.start();
        Thread.sleep(10); //give kafka consumer time to start and poll
        verify(mockedTaskManager, atLeastOnce()).executeTasks(anyString(), any());
    }

    @Test
    void testThatKafkaConsumerCommitsOffsetAfterProcessingProperly() throws InterruptedException {
        kafkaConsumer.start();
        Thread.sleep(10);
        verify(mockedKafkaConsumer, atLeastOnce()).commitAsync();
    }
}
