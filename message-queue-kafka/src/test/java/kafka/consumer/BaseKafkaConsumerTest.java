package kafka.consumer;

import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.taskmanager.TaskManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BaseKafkaConsumerTest {
    @InjectMocks
    private ExampleBaseKafkaConsumer exampleBaseKafkaConsumer;
    @Mock
    private KafkaConsumer<String, String> mockedKafkaConsumer;
    @Mock
    private KafkaProducer<String, String> mockedKafkaProducer;
    @Mock
    private KafkaMessageBrokerProxy mockedKafkaMessageBrokerProxy;
    @Mock
    private ConsumerProperties consumerProperties;
    @Mock
    private TaskManager taskManager;
    private final String topicName = "test";
    private final String unsubscribeName = "test1";
    private Set<String> fakeSubscriptionList;
    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        fakeSubscriptionList = new HashSet<>();
        fakeSubscriptionList.add(unsubscribeName);
        fakeSubscriptionList.add("test2");
        when(mockedKafkaConsumer.subscription()).thenReturn(fakeSubscriptionList);
        exampleBaseKafkaConsumer = new ExampleBaseKafkaConsumer(mockedKafkaMessageBrokerProxy, mockedKafkaConsumer, consumerProperties, taskManager);
    }

    @Test
    void testThatSubscribingWorksCorrectly() {
        exampleBaseKafkaConsumer.subscribe(topicName);
        fakeSubscriptionList.add(topicName);
        verify(mockedKafkaConsumer).subscribe(fakeSubscriptionList);
    }

    @Test
    void testThatSubscribingInBatchesWorksCorrectly() {
        Set<String> newSubs = Set.of("test3", "test4");
        exampleBaseKafkaConsumer.subscribe(newSubs);
        fakeSubscriptionList.addAll(newSubs);

        verify(mockedKafkaConsumer).subscribe(fakeSubscriptionList);
    }

    @Test
    void testThatUnsubscribingWorksCorrectly() {
        exampleBaseKafkaConsumer.unsubscribe(unsubscribeName);

        verify(mockedKafkaConsumer).subscribe(fakeSubscriptionList);
    }

    @Test
    void testThatUnsubscribingInBatchesWorksCorrectly() {
        Set<String> removedSubs = Set.of("test1", "test2");
        exampleBaseKafkaConsumer.unsubscribe(removedSubs);
        fakeSubscriptionList.removeAll(removedSubs);
        verify(mockedKafkaConsumer).subscribe(fakeSubscriptionList);
    }

    @Test
    void testThatCheckingForSubscriptionWorksCorrectly() {
        Assertions.assertTrue(exampleBaseKafkaConsumer.isSubscribed(unsubscribeName));
        Assertions.assertFalse(exampleBaseKafkaConsumer.isSubscribed("a fake topic name"));
    }
}
