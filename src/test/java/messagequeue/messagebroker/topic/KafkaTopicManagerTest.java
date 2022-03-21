package messagequeue.messagebroker.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTopicManagerTest {
    @InjectMocks
    private KafkaTopicManager kafkaTopicManager;
    @Mock
    private Admin mockedAdmin;
    @Mock
    private ListTopicsResult listTopicsResult;
    @Mock
    private KafkaFuture<Collection<TopicListing>> mockedFuture;
    @Mock
    private CreateTopicsResult mockedCreateTopicsResult;
    @Mock
    private DeleteTopicsResult mockedDeleteTopicsResult;
    @Mock
    private KafkaFuture<Void> mockedVoidFuture;
    @Captor
    private ArgumentCaptor<Set<NewTopic>> newTopicSetCaptor;
    @Captor
    private ArgumentCaptor<Collection<String>> topicListCaptor;

    private List<String> fakeTopicList = List.of("unit", "testing", "is", "so", "fun");
    private final String notExistingTopicName = "unique name";
    private final int partitions = 123;
    private final int replicationFactor = 321;
    private Map<String, KafkaFuture<Void>> futureMap;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        MockitoAnnotations.openMocks(this);
        futureMap = Map.of(notExistingTopicName, mockedVoidFuture);
        kafkaTopicManager = new KafkaTopicManager(mockedAdmin);
        when(mockedAdmin.listTopics()).thenReturn(listTopicsResult);
        when(mockedAdmin.deleteTopics(anyCollection())).thenReturn(mockedDeleteTopicsResult);
        when(listTopicsResult.listings()).thenReturn(mockedFuture);
        when(mockedFuture.get()).thenReturn(fakeTopicList.stream().map(topicName -> new TopicListing(topicName, Uuid.randomUuid(), false)).toList());
        when(mockedAdmin.createTopics(anyCollection())).thenReturn(mockedCreateTopicsResult);
        when(mockedDeleteTopicsResult.topicNameValues()).thenReturn(futureMap);
        when(mockedCreateTopicsResult.values()).thenReturn(futureMap);
    }

    @Test
    void testThatCreatingANewTopicConfiguresTheTopicCorrectly() {
        TopicConfiguration topicConfiguration = new KafkaTopicConfiguration(notExistingTopicName, partitions, replicationFactor);
        kafkaTopicManager.createTopic(topicConfiguration);
        Mockito.verify(mockedAdmin).createTopics(newTopicSetCaptor.capture());
        NewTopic topic = newTopicSetCaptor.getValue().stream().toList().get(0);

        Assertions.assertEquals(topic.name(), notExistingTopicName);
        Assertions.assertEquals(topic.numPartitions(), partitions);
        Assertions.assertEquals(topic.replicationFactor(), replicationFactor);
    }

    @Test
    void testThatCreatingATopicWithANameThatAlreadyExistsThrowsAFailedTopicActionException() {
        TopicConfiguration topicConfiguration = new KafkaTopicConfiguration("unit", partitions, replicationFactor);
        FailedTopicActionException exception = Assertions.assertThrows(FailedTopicActionException.class, () -> kafkaTopicManager.createTopic(topicConfiguration));
        Assertions.assertTrue(exception.getMessage().contains("exists"));
    }

    @Test
    void testThatCreatingATopicThatFailsThrowsAFailedTopicActionException() throws ExecutionException, InterruptedException {
        TopicConfiguration topicConfiguration = new KafkaTopicConfiguration(notExistingTopicName, partitions, replicationFactor);
        when(mockedVoidFuture.get()).thenThrow(ExecutionException.class);
        FailedTopicActionException exception = Assertions.assertThrows(FailedTopicActionException.class, () -> kafkaTopicManager.createTopic(topicConfiguration));
        Assertions.assertTrue(exception.getMessage().contains("failed"));
    }

    @Test
    void testThatRemovingATopicPassesInTheCorrectTopic() {
        kafkaTopicManager.removeTopic(notExistingTopicName);
        Mockito.verify(mockedAdmin).deleteTopics(topicListCaptor.capture());
        List<String> topicList = topicListCaptor.getValue().stream().toList();
        Assertions.assertEquals(1, topicList.size());
        Assertions.assertTrue(topicList.contains(notExistingTopicName));
    }

    @Test
    void testThatAFailedDeletionThrowsAFailedTopicActionException() throws ExecutionException, InterruptedException {
        when(mockedVoidFuture.get()).thenThrow(ExecutionException.class);
        FailedTopicActionException exception = Assertions.assertThrows(FailedTopicActionException.class, () -> kafkaTopicManager.removeTopic(notExistingTopicName));

        Assertions.assertTrue(exception.getMessage().contains("not delete"));
    }

    @Test
    void testThatGettingTopicsMapsCorrectly() {
        List<String> topics = kafkaTopicManager.getTopics();

        Assertions.assertEquals(topics, fakeTopicList);
    }

    @Test
    void testThatAFailedTopicListRetrievalThrowsAFailedTopicActionException() throws ExecutionException, InterruptedException {
        when(mockedFuture.get()).thenThrow(ExecutionException.class);
        FailedTopicActionException exception = Assertions.assertThrows(FailedTopicActionException.class, () -> kafkaTopicManager.getTopics());

        Assertions.assertTrue(exception.getMessage().contains("Getting all topics failed"));
    }

    @Test
    void testThatCheckingForTopicExistenceWorksCorrectly() {
        Assertions.assertTrue(kafkaTopicManager.topicExists("unit"));
        Assertions.assertFalse(kafkaTopicManager.topicExists(notExistingTopicName));
    }
}
