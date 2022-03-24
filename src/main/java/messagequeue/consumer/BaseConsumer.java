package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.Consumer;
import messagequeue.messagebroker.MessageBrokerProxy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A base consumer class that all consumer should inherit from. This class takes care of all consumer related tasks except for {@link Consumer#consume()} which is implementation specific.
 */
public abstract class BaseConsumer implements Consumer {
    protected final String name;
    protected MessageBrokerProxy messageBrokerProxy;
    protected final AtomicBoolean scheduledForRemoval = new AtomicBoolean();
    protected final AtomicBoolean isRunning = new AtomicBoolean();
    protected final AtomicInteger numberOfConcurrentRunningTasks = new AtomicInteger();
    protected final TaskManager taskManager;

    protected BaseConsumer(MessageBrokerProxy messageBrokerProxy, ConsumerProperties consumerProperties, TaskManager taskManager) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = consumerProperties.name();
        this.taskManager = taskManager;
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void start() {
        scheduledForRemoval.set(false);
        isRunning.set(true);
        new Thread(this::consume).start();
    }

    @Override
    public void stop() {
        scheduledForRemoval.set(true);
    }

    @Override
    public String getIdentifier() {
        return name;
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public int getNumberOfRunningTasks() {
        return numberOfConcurrentRunningTasks.get();
    }

    protected Runnable createTask(String message) {
        return () -> {
            numberOfConcurrentRunningTasks.incrementAndGet();
            process(message);
            numberOfConcurrentRunningTasks.decrementAndGet();
        };
    }
}