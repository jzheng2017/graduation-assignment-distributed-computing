package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FakeBaseConsumer extends BaseConsumer{
    protected FakeBaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        super(scheduledForRemoval, isRunning, taskManager);
    }

    @Override
    public void acknowledge(List<TaskPackageResult> taskPackageResults) {

    }

    @Override
    public Map<String, List<String>> poll() {
        return new HashMap<>();
    }

    @Override
    public void cleanup() {

    }
}
