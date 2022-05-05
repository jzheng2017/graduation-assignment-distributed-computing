package messagequeue.consumer;

import datastorage.KVClient;
import datastorage.LockClient;
import datastorage.configuration.KeyPrefix;
import datastorage.configuration.LockName;
import messagequeue.Util;
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
    private Util util;

    //only for unit test purposes
    protected BaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        this.name = "unit test";
        this.scheduledForRemoval = scheduledForRemoval;
        this.isRunning = isRunning;
        this.taskManager = taskManager;
        this.messageProcessor = null;
        this.isInternal = true;
    }

    protected BaseConsumer(String name, boolean isInternal, TaskManager taskManager, MessageProcessor messageProcessor, KVClient kvClient, LockClient lockClient, Util util) {
        this.name = name;
        this.messageProcessor = messageProcessor;
        this.taskManager = taskManager;
        this.kvClient = kvClient;
        this.lockClient = lockClient;
        this.util = util;
        this.scheduledForRemoval = new AtomicBoolean();
        this.isRunning = new AtomicBoolean();
        this.isInternal = isInternal;
        logger.info("Creating consumer {}..", name);
    }

    protected BaseConsumer(String name, boolean isInternal, TaskManager taskManager, MessageProcessor messageProcessor, KVClient kvClient, LockClient lockClient, Util util, List<TopicOffset> topicOffsets) {
        this(name, isInternal, taskManager, messageProcessor, kvClient, lockClient, util);
        setTopicOffsets(topicOffsets);
    }

    private void setTopicOffsets(List<TopicOffset> topicOffsets) {
        topicOffsets.forEach(topicOffset -> commitOffset(topicOffset.topic(), topicOffset.offset()));
        logger.info("Topic offsets set for consumer '{}'", name);
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
        taskManager.cancelConsumerTasks(name);
        isRunning.set(false);
    }

    @Override
    public void consume() {
        while (!scheduledForRemoval.get()) {
            List<TaskPackage> taskPackages = poll().entrySet().stream().map(entry -> createTaskPackage(entry.getKey(), entry.getValue())).toList();

            if (!taskPackages.isEmpty()) {
                try {
                    logger.info("Consumer '{}' dispatched {} task package(s) to the TaskManager for processing", name, taskPackages.size());
                    taskManager.executeTasks(name, taskPackages);
                    logger.info("{} task package(s) successfully processed by consumer '{}'", taskPackages.size(), name);
                } catch (InterruptedException e) {
                    logger.warn("Execution of the tasks has been interrupted. Unfinished tasks have been cancelled", e); //TODO: Tasks that are finished should be committed
                }
                acknowledge(taskPackages.stream().map(taskPackage -> new TaskPackageResult(taskPackage.getTopicName(), taskPackage.isFinished() && !taskPackage.isCancelled(), taskPackage.getTotalProcessed())).toList());
            }
        }

        cleanup();
        logger.info("Closed consumer '{}' and stopped running", name);
    }

    @Override
    public List<TopicOffset> listTopicOffsets() {
        try {
            return kvClient
                    .getByPrefix(KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + name)
                    .get()
                    .keyValues()
                    .entrySet()
                    .stream()
                    .map(
                            topicOffset -> {
                                String topic = util.getSubstringAfterPrefix(KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + name + "-", topicOffset.getKey());
                                long offset = Long.parseLong(topicOffset.getValue());
                                return new TopicOffset(topic, offset);
                            }
                    )
                    .toList();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not retrieve topic offsets of consumer '{}'", name);
            return new ArrayList<>();
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
    public long getTopicOffset(String topic) {
        final String key = KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + name + "-" + topic;
        try {
            final String commitOffset = kvClient.get(key).get().keyValues().get(key);

            if (commitOffset == null) {
                commitOffset(topic, 0);
                return 0;
            }

            return Long.parseLong(commitOffset);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Could not get commit offset of consumer '{}' of topic '{}'", name, topic);
            throw new IllegalArgumentException("Could not get commit offset");
        }
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

        return new TaskPackage(name, topic, tasks);
    }

    private Task createTask(String topic, String message) {
        return new Task(() -> {
            messageProcessor.process(message);
            commitOffset(topic, getTopicOffset(topic) + 1); //no lock needed as only the consumer itself will modify this value (sequentially), so there will never be a case where two entities modify it at the same time
        });
    }

    private void commitOffset(String topic, long offset) {
        try {
            kvClient.put(KeyPrefix.CONSUMER_TOPIC_OFFSET + "-" + name + "-" + topic, Long.toString(offset)).get();
            logger.trace("Topic offset set to {} for topic '{}' of consumer '{}'", offset, topic, name);
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Committing offset for topic '{}' of consumer '{}' was not successful", topic, name);
        }
    }
}