package messagequeue.consumer;

import messagequeue.consumer.taskmanager.TaskManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FakeBaseConsumer extends BaseConsumer{
    protected FakeBaseConsumer(AtomicBoolean scheduledForRemoval, AtomicBoolean isRunning, TaskManager taskManager) {
        super(scheduledForRemoval, isRunning, taskManager);
    }

    @Override
    public void acknowledge() {

    }

    @Override
    public void commitOffset(String topic, int offset) {

    }

    @Override
    public int getCommitOffset(String topic) {
        return 0;
    }

    @Override
    public Map<String, List<String>> poll() {
        return new HashMap<>();
    }

    @Override
    public void cleanup() {

    }
}
