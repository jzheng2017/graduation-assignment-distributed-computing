package messagequeue.consumer;

import messagequeue.consumer.taskmanager.Task;
import messagequeue.consumer.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

    //only for unit test purposes
    protected BaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        this.name = "unit test";
        this.scheduledForRemoval = scheduledForRemoval;
        this.isRunning = isRunning;
        this.taskManager = taskManager;
        this.messageProcessor = null;
        this.isInternal = true;
    }

    protected BaseConsumer(String name, boolean isInternal, TaskManager taskManager, MessageProcessor messageProcessor) {
        this.name = name;
        this.messageProcessor = messageProcessor;
        this.taskManager = taskManager;
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
    }

    @Override
    public void consume() {
        while (!scheduledForRemoval.get()) {
            List<Task> tasksToBeExecuted = poll().stream().map(this::createTask).toList();

            if (tasksToBeExecuted.size() > 0) {
                try {
                    taskManager.executeTasks(tasksToBeExecuted);
                    logger.info("Consumer '{}' created {} new task(s) and will be dispatched for execution", name, tasksToBeExecuted.size());
                    logger.info("{} tasks successfully processed by consumer '{}'", tasksToBeExecuted.size(), name);
                    acknowledge();
                } catch (InterruptedException e) {
                    logger.warn("Execution of the tasks has been interrupted. Unfinished tasks have been cancelled", e); //TODO: Tasks that are finished should be committed
                }
            }
        }

        cleanup();
        isRunning.set(false);
        logger.info("Closed consumer '{}' and stopped running", name);
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

    private Task createTask(String message) {
        return new Task(name, () -> messageProcessor.process(message));
    }
}