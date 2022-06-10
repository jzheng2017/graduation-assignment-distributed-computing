package messagequeue.consumer;

import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import messagequeue.consumer.builder.ConsumerConfigurationReader;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.subscription.SubscriptionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

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
    @Mock
    private ConsumerConfigurationReader mockedConsumerConfigurationReader;
    @Mock
    private ConsumerConfigurationParser consumerConfigurationParser;
    @Mock
    private SubscriptionManager subscriptionManager;
    private String consumerIdentifier = "steve rogers";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        consumerManager = new ConsumerManagerImpl(mockedLogger, mockedTaskManager, mockedConsumerConfigurationReader, mockedConsumerBuilder, consumerConfigurationParser, subscriptionManager);
        when(mockedConsumerBuilder.createConsumer("test")).thenReturn(mockedConsumer);
        when(mockedConsumer.getIdentifier()).thenReturn(consumerIdentifier);
        when(mockedConsumerConfigurationReader.getConsumerConfiguration(consumerIdentifier)).thenReturn("test");
    }

    @Test
    void testThatRegisteringAConsumerPutsItCorrectly() {
        consumerManager.registerConsumer(consumerIdentifier);
        List<String> consumers = consumerManager.getAllConsumers();

        Assertions.assertTrue(consumers.contains(mockedConsumer.getIdentifier()));
    }

    @Test
    void testThatRegisteringAlreadyRegisteredConsumerLogsIt() {
        consumerManager.registerConsumer(consumerIdentifier);
        consumerManager.registerConsumer(consumerIdentifier);
        verify(mockedLogger).warn("Consumer '{}' has already been registered.", consumerIdentifier);
    }

    @Test
    void testThatRegisteringAConsumerAlsoStartsIt() {
        consumerManager.registerConsumer(consumerIdentifier);

        verify(mockedConsumer).start();
    }

    @Test
    void testThatStartingAConsumerThatIsNotRegisteredLogsAWarning() {
        consumerManager.startConsumer("does not exist");

        verify(mockedLogger).warn("Consumer '{}' can not be started because is has not been registered yet.", "does not exist");
    }

    @Test
    void testThatStartingAConsumerThatIsAlreadyStartedLogsAWarning() {
        consumerManager.registerConsumer(consumerIdentifier);
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
        consumerManager.registerConsumer(consumerIdentifier);
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
        consumerManager.registerConsumer(consumerIdentifier);

        Assertions.assertTrue(consumerManager.getAllConsumers().contains(mockedConsumer.getIdentifier()));
        when(mockedConsumer.isRunning())
                .thenReturn(true)//first isRunning check in the stopConsumer()
                .thenReturn(false); //second isRunning check in the while loop in unregisterConsumer()
        consumerManager.unregisterConsumer(consumerIdentifier);
        verify(mockedConsumer).stop();
        Assertions.assertFalse(consumerManager.getAllConsumers().contains(mockedConsumer.getIdentifier()));
    }

    @Test
    void testThatCheckingWhetherAConsumerIsInternalWorksCorrectly() {
        consumerManager.registerConsumer(consumerIdentifier);
        when(mockedConsumer.isInternal()).thenReturn(true).thenReturn(false);
        Assertions.assertTrue(consumerManager.isConsumerInternal(consumerIdentifier));
        Assertions.assertFalse(consumerManager.isConsumerInternal(consumerIdentifier));
    }
}
