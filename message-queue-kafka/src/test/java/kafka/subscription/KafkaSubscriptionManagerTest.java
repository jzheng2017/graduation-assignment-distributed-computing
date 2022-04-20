package kafka.subscription;

import kafka.consumer.KafkaConsumer;
import messagequeue.messagebroker.subscription.Subscription;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaSubscriptionManagerTest {
    private KafkaSubscriptionManager kafkaSubscriptionManager;
    @Mock
    private Consumer<String, String> mockedConsumer;
    @Mock
    private KafkaConsumer mockedKafkaConsumer;
    private Map<String, Object> consumerContext;
    private String consumerName = "kafka";
    private Set<String> subscriptions = Set.of("a", "b", "c");
    private Set<String> topics = Set.of("d", "e", "f");
    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        kafkaSubscriptionManager = new KafkaSubscriptionManager();
        consumerContext = new HashMap<>();
        consumerContext.put("name", consumerName);
        consumerContext.put("consumer", mockedKafkaConsumer);
        when(mockedKafkaConsumer.getConsumer()).thenReturn(mockedConsumer);
        when(mockedConsumer.subscription()).thenReturn(subscriptions);
    }

    @Test
    void testThatSubscriptionsAreMappedCorrectly() {
        Set<Subscription> actualSubscriptions = kafkaSubscriptionManager.getSubscriptions(consumerContext);
        Assertions.assertEquals(subscriptions.stream().map(Subscription::new).collect(Collectors.toSet()), actualSubscriptions);
    }

    @Test
    void testThatSubscribingWorksCorrectly() {
        kafkaSubscriptionManager.subscribe(topics, consumerContext);
        Set<String> expectedSubscriptions = new HashSet<>();

        expectedSubscriptions.addAll(topics);
        expectedSubscriptions.addAll(subscriptions);

        verify(mockedConsumer).subscribe(expectedSubscriptions);
    }

    @Test
    void testThatUnsubscribingWorksCorrectly() {
        Set<String> expectedSubscriptions = new HashSet<>(subscriptions);
        Set<String> topicsToUnsubscribeFrom = Set.of("a");
        expectedSubscriptions.removeAll(topicsToUnsubscribeFrom);

        kafkaSubscriptionManager.unsubscribe(topicsToUnsubscribeFrom, consumerContext);

        verify(mockedConsumer).subscribe(expectedSubscriptions);
    }

    @Test
    void testThatCheckingForSubscriptionWorksCorrectly() {
        Assertions.assertTrue(kafkaSubscriptionManager.isSubscribed("a", consumerContext));
        Assertions.assertFalse(kafkaSubscriptionManager.isSubscribed("d", consumerContext));
    }
}
