package messagequeue.consumer.taskmanager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A task package that consist of one or more tasks that have to be processed. Messages from a single topic are packaged into the same {@link TaskPackage} to ensure sequential order.
 */
public class TaskPackage implements Runnable {
    private String consumerId;
    private String topicName;
    private List<Task> tasks;
    private AtomicInteger position = new AtomicInteger();
    private volatile boolean cancelled = false;
    private volatile boolean finished = false;

    public TaskPackage(String consumerId, String topicName, List<Task> tasks) {
        this.consumerId = consumerId;
        this.topicName = topicName;
        this.tasks = tasks;
    }

    @Override
    public void run() {
        Task task;
        while ((task = getNextTask().orElse(null)) != null) {
            task.task().run();
        }
        finished = true;
    }

    public void cancel() {
        this.cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isFinished() {
        return finished;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getTopicName() {
        return topicName;
    }

    private Optional<Task> getNextTask() {
        if (!cancelled) {
            return Optional.ofNullable(tasks.get(position.getAndIncrement()));
        } else {
            return Optional.empty();
        }
    }
}
