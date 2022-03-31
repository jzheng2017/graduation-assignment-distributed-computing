package kafka.consumer;

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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConsumerTest {
    @InjectMocks
    private kafka.consumer.KafkaConsumer kafkaConsumer;
    @Mock
    private KafkaConsumer<String, String> mockedKafkaConsumer;
    @Mock
    private TaskManager mockedTaskManager;
    @Mock
    private MessageProcessor mockedMessageProcessor;
    private ConsumerRecords<String, String> consumerRecords;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        consumerRecords = new ConsumerRecords<>(Map.of(topicPartition, List.of(new ConsumerRecord<>("test", 0, 0L, "key", "value"))));
        kafkaConsumer = new kafka.consumer.KafkaConsumer(mockedKafkaConsumer, "kafka", mockedTaskManager, mockedMessageProcessor);
        when(mockedKafkaConsumer.poll(any())).thenReturn(consumerRecords);

        kafkaConsumer.start();
    }


    @Test
    void testThatPollReturnsCorrectMessages() {
        List<String> polledMessages = kafkaConsumer.poll();

        Assertions.assertEquals(1, polledMessages.size());
        Assertions.assertTrue(polledMessages.contains("value"));
    }

    @Test
    void testThatKafkaConsumerClosesConsumerWhenStopped() throws InterruptedException {
        kafkaConsumer.stop();
        Thread.sleep(1); //give kafka consumer time to stop properly
        verify(mockedKafkaConsumer).close();
    }

    @Test
    void testThatKafkaConsumerDispatchesTasks() throws InterruptedException {
        Thread.sleep(1); //give kafka consumer time to start and poll
        verify(mockedTaskManager, atLeastOnce()).executeTasks(any());
    }

    @Test
    void testThatKafkaConsumerCommitsOffsetAfterProcessingProperly() throws InterruptedException {
        Thread.sleep(2);
        verify(mockedKafkaConsumer, atLeastOnce()).commitAsync();
    }
}
