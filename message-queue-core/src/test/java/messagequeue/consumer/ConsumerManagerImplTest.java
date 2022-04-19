package messagequeue.consumer;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.taskmanager.TaskManager;
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

class ConsumerManagerImplTest {
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
        consumerManager = new ConsumerManagerImpl(mockedLogger, mockedTaskManager);
        when(mockedConsumerBuilder.createConsumer(anyString())).thenReturn(mockedConsumer);
        when(mockedConsumer.getIdentifier()).thenReturn(consumerIdentifier);
    }

    @Test
    void testThatRegisteringAConsumerPutsItCorrectly() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
        List<Consumer> consumers = consumerManager.getAllConsumers();

        Assertions.assertTrue(consumers.contains(mockedConsumer));
    }

    @Test
    void testThatRegisteringAlreadyRegisteredConsumerLogsIt() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
        verify(mockedLogger).warn("Consumer '{}' has already been registered.", consumerIdentifier);
    }

    @Test
    void testThatRegisteringAConsumerAlsoStartsIt() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));

        verify(mockedConsumer).start();
    }

    @Test
    void testThatStartingAConsumerThatIsNotRegisteredLogsAWarning() {
        consumerManager.startConsumer("does not exist");

        verify(mockedLogger).warn("Consumer '{}' can not be started because is has not been registered yet.", "does not exist");
    }

    @Test
    void testThatStartingAConsumerThatIsAlreadyStartedLogsAWarning() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
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
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
        when(mockedConsumer.isRunning()).thenThrow(RuntimeException.class).thenReturn(true); //throw an exception because the consumer needs to stay in the consumer list for the second unregister
        try {
            consumerManager.unregisterConsumer(consumerIdentifier);
        } catch (Exception ignored) {
        }
        consumerManager.unregisterConsumer(consumerIdentifier);

        verify(mockedLogger).warn("Consumer '{}' is already scheduled for removal", consumerIdentifier);
    }

    @Test
    void testThatUnregisteringAConsumerStopsTheConsumerAndRemovesItFromTheConsumerList() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));

        Assertions.assertTrue(consumerManager.getAllConsumers().contains(mockedConsumer));
        when(mockedConsumer.isRunning())
                .thenReturn(true)//first isRunning check in the stopConsumer()
                .thenReturn(false); //second isRunning check in the while loop in unregisterConsumer()
        consumerManager.unregisterConsumer(consumerIdentifier);
        verify(mockedConsumer).stop();
        Assertions.assertFalse(consumerManager.getAllConsumers().contains(mockedConsumer));
    }

    @Test
    void testThatCheckingWhetherAConsumerIsInternalWorksCorrectly() {
        consumerManager.registerConsumer(mockedConsumerBuilder.createConsumer(""));
        when(mockedConsumer.isInternal()).thenReturn(true).thenReturn(false);
        Assertions.assertTrue(consumerManager.isConsumerInternal(consumerIdentifier));
        Assertions.assertFalse(consumerManager.isConsumerInternal(consumerIdentifier));
    }
}
