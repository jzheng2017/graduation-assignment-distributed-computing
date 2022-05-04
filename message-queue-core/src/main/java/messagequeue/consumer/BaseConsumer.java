package messagequeue.consumer;

import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.KeyPrefix;
import datastorage.configuration.LockName;
import messagequeue.consumer.taskmanager.Task;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.consumer.taskmanager.TaskPackage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base consumer class that all consumer should inherit from. This class takes care of all consumer related tasks except for {@link Consumer#consume()} which is implementation specific.
 */
public abstract class BaseConsumer implements Consumer {
    private Logger logger = LoggerFactory.getLogger(BaseConsumer.class);
    protected final String name;
    protected final AtomicBoolean scheduledForRemoval;
    protected final AtomicBoolean isRunning;
    private final MessageProcessor messageProcessor;
    private final TaskManager taskManager;
    private final boolean isInternal;
    private KVClient kvClient;
    private LockClient lockClient;

    //only for unit test purposes
    protected BaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        this.name = "unit test";
        this.scheduledForRemoval = scheduledForRemoval;
        this.isRunning = isRunning;
        this.taskManager = taskManager;
        this.messageProcessor = null;
        this.isInternal = true;
    }

    protected BaseConsumer(String name, boolean isInternal, TaskManager taskManager, MessageProcessor messageProcessor, KVClient kvClient, LockClient lockClient) {
        this.name = name;
        this.messageProcessor = messageProcessor;
        this.taskManager = taskManager;
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.scheduledForRemoval = new AtomicBoolean();
        this.isRunning = new AtomicBoolean();
        this.isInternal = isInternal;
        logger.info("Creating consumer {}..", name);
    }

    @Override
    public void start() {
        scheduledForRemoval.set(false);
        isRunning.set(true);
        logger.info("Consumer {} started. It will now start consuming new messages.", name);
        new Thread(this::consume).start();
    }

    @Override
    public void stop() {
        scheduledForRemoval.set(true);
        isRunning.set(false);
    }

    @Override
    public void consume() {
        while (!scheduledForRemoval.get()) {
            List<TaskPackage> taskPackages = poll().entrySet().stream().map(entry -> createTaskPackage(entry.getKey(), entry.getValue())).toList();

            if (!taskPackages.isEmpty()) {
                try {
                    logger.info("Consumer '{}' dispatched {} task packages to the TaskManager for processing", name, taskPackages.size());
                    taskManager.executeTasks(taskPackages);
                    logger.info("{} tasks successfully processed by consumer '{}'", taskPackages.size(), name);
                    acknowledge();
                } catch (InterruptedException e) {
                    logger.warn("Execution of the tasks has been interrupted. Unfinished tasks have been cancelled", e); //TODO: Tasks that are finished should be committed
                }
            }
        }

        cleanup();
        logger.info("Closed consumer '{}' and stopped running", name);
    }

    @Override
    public void commitOffset(String topic, int offset) {
        try {
            kvClient.put(KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + getIdentifier() + "-" + topic, Integer.toString(offset)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Committing offset for topic '{}' of consumer '{}' was not successful", topic, getIdentifier());
        }
    }

    @Override
    public int getCommitOffset(String topic) {
        final String key = KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + getIdentifier() + "-" + topic;
        try {
            final String commitOffset = kvClient.get(key).get().keyValues().get(key);

            if (commitOffset == null) {
                commitOffset(topic, 0);
                return 0;
            }

            return Integer.parseInt(commitOffset);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Could not get commit offset of consumer '{}' of topic '{}'", getIdentifier(), topic);
            throw new IllegalArgumentException("Could not get commit offset");
        }
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
    public boolean isInternal() {
        return isInternal;
    }

    private TaskPackage createTaskPackage(String topic, List<String> messages) {
        List<Task> tasks = new ArrayList<>();

        for (String message : messages) {
            tasks.add(createTask(topic, message));
        }

        return new TaskPackage(getIdentifier(), topic, tasks);
    }

    private Task createTask(String topic, String message) {
        return new Task(() -> {
            messageProcessor.process(message);
            lockClient.acquireLockAndExecute(
                    LockName.CONSUMER_OFFSET_LOCK + "-" + getIdentifier() + "-" + topic,
                    () -> {
                        commitOffset(topic, getCommitOffset(topic) + 1);
                        return null;
                    }
            );
        });
    }
}