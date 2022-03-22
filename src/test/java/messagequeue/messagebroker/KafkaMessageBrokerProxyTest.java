package messagequeue.messagebroker;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class KafkaMessageBrokerProxyTest {
    @InjectMocks
    private KafkaMessageBrokerProxy kafkaMessageBrokerProxy;
    @Mock
    private Producer<String, String> mockedProducer;
    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor;
    private final String topicName = "test";
    private final String message = "a test message";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        kafkaMessageBrokerProxy = new KafkaMessageBrokerProxy(mockedProducer);
    }

    @Test
    void testThatSendingMessageExecutesCorrectly() {
        kafkaMessageBrokerProxy.sendMessage(topicName, message);
        Mockito.verify(mockedProducer).send(producerRecordCaptor.capture());
        ProducerRecord<String, String> actualProducerRecord = producerRecordCaptor.getValue();

        Mockito.verify(mockedProducer).flush();
        Assertions.assertEquals(topicName, actualProducerRecord.topic());
        Assertions.assertEquals(message, actualProducerRecord.value());
    }
}