package messagequeue.consumer;

import messagequeue.messagebroker.Consumer;
import messagequeue.messagebroker.MessageBrokerProxy;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseConsumer implements Consumer {
    protected final String name;
    protected MessageBrokerProxy messageBrokerProxy;
    protected final AtomicBoolean scheduledForRemoval = new AtomicBoolean();
    protected final AtomicBoolean isRunning = new AtomicBoolean();
    protected final AtomicInteger numberOfConcurrentRunningTasks = new AtomicInteger();
    protected ExecutorService executor;

    protected BaseConsumer(MessageBrokerProxy messageBrokerProxy, ConsumerProperties consumerProperties) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.name = consumerProperties.getName();
        this.executor = Executors.newFixedThreadPool(consumerProperties.getThreadPoolSize());
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void start() {
        isRunning.set(true);
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

    protected Callable<Void> createTask(String message) {
        return () -> {
            numberOfConcurrentRunningTasks.incrementAndGet();
            process(message);
            numberOfConcurrentRunningTasks.decrementAndGet();
            return null;
        };
    }
}