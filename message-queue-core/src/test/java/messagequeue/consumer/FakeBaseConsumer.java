package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FakeBaseConsumer extends BaseConsumer{
    protected FakeBaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        super(scheduledForRemoval, isRunning, taskManager);
    }

    @Override
    public void acknowledge() {

    }

    @Override
    public List<String> poll() {
        return new ArrayList<>();
    }

    @Override
    public void cleanup() {

    }
}
