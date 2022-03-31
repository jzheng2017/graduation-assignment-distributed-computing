package messagequeue.consumer;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerManagerImplTest {
    @InjectMocks
    private ConsumerManagerImpl consumerManager;
    @Mock
    private Consumer mockedConsumer;
    @Mock
    private TaskManager mockedTaskManager;
    @Mock
    private ConsumerBuilder mockedConsumerBuilder;
    @Mock
    private Logger mockedLogger;
    private String consumerIdentifier = "steve rogers";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        consumerManager = new ConsumerManagerImpl(mockedLogger, mockedConsumerBuilder, mockedTaskManager);
        when(mockedConsumerBuilder.createConsumer(anyString())).thenReturn(mockedConsumer);
        when(mockedConsumer.getIdentifier()).thenReturn(consumerIdentifier);
    }

    @Test
    void testThatRegisteringAConsumerPutsItCorrectly() {
        consumerManager.registerConsumer("");
        List<Consumer> consumers = consumerManager.getAllConsumers();

        Assertions.assertTrue(consumers.contains(mockedConsumer));
    }

    @Test
    void testThatRegisteringAlreadyRegisteredConsumerLogsIt() {
        consumerManager.registerConsumer("");
        consumerManager.registerConsumer("");
        verify(mockedLogger).warn("Consumer '{}' has already been registered.", consumerIdentifier);
    }

    @Test
    void testThatRegisteringAConsumerAlsoStartsIt() {
        consumerManager.registerConsumer("");

        verify(mockedConsumer).start();
    }

    @Test
    void testThatStartingAConsumerThatIsNotRegisteredLogsAWarning() {
        consumerManager.startConsumer("does not exist");

        verify(mockedLogger).warn("Consumer '{}' can not be started because is has not been registered yet.", "does not exist");
    }

    @Test
    void testThatStartingAConsumerThatIsAlreadyStartedLogsAWarning() {
        consumerManager.registerConsumer("");
        when(mockedConsumer.isRunning()).thenReturn(true);

        consumerManager.startConsumer(consumerIdentifier);

        verify(mockedLogger).warn("Consumer '{}' is already running.", consumerIdentifier);
    }

    @Test
    void testThatUnregisteringANonRegisteredConsumerLogsAWarning() {
        consumerManager.unregisterConsumer("does not exist");
        verify(mockedLogger).warn("Consumer '{}' can not be unregistered because it has not been registered", "does not exist");
    }

    @Test
    void testThatUnregisteringAConsumerThatIsAlreadyScheduledForRemovalLogsAWarning() {
        consumerManager.registerConsumer("");
        new Thread(() -> consumerManager.unregisterConsumer(consumerIdentifier)).start(); //start in a new thread which should loop endlessly as consumer.isRunning() defaults to false as it's a mocked object
        consumerManager.unregisterConsumer(consumerIdentifier);
        verify(mockedLogger).warn("Consumer '{}' is already scheduled for removal", consumerIdentifier);
    }

    @Test
    void testThatUnregisteringAConsumerStopsTheConsumerAndRemovesItFromTheConsumerList() {
        consumerManager.registerConsumer("");

        Assertions.assertTrue(consumerManager.getAllConsumers().contains(mockedConsumer));
        when(mockedConsumer.isRunning())
                .thenReturn(true)//first isRunning check in the stopConsumer()
                .thenReturn(false); //second isRunning check in the while loop in unregisterConsumer()
        consumerManager.unregisterConsumer(consumerIdentifier);
        verify(mockedConsumer).stop();
        Assertions.assertFalse(consumerManager.getAllConsumers().contains(mockedConsumer));
    }
}
