package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.atomic.AtomicBoolean;

class BaseConsumerTest {
    @InjectMocks
    private FakeBaseConsumer baseConsumer;
    private AtomicBoolean isRunning;
    private AtomicBoolean isScheduledForRemoval;
    @Mock
    private TaskManager mockedTaskedManager;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        isRunning = new AtomicBoolean();
        isScheduledForRemoval = new AtomicBoolean();
        baseConsumer = new FakeBaseConsumer(isScheduledForRemoval, isRunning, mockedTaskedManager);
    }

    @Test
    void testThatStartingTheConsumerIsDoneCorrectly() {
        baseConsumer.start();
        Assertions.assertTrue(isRunning.get());
        Assertions.assertFalse(isScheduledForRemoval.get());
    }

    @Test
    void testThatStoppingTheConsumerIsDoneCorrectly() {
        baseConsumer.stop();
        Assertions.assertTrue(isScheduledForRemoval.get());
    }

    @Test
    void testThatStoppingTheConsumerSetsTheRunningFlagToFalse() throws InterruptedException {
        baseConsumer.start();
        Assertions.assertTrue(isRunning.get());
        baseConsumer.stop();
        Thread.sleep(5); //give the consumer time to stop
        Assertions.assertFalse(isRunning.get());
    }
}
