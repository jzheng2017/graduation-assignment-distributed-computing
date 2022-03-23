package messagequeue.consumer.taskmanager;

import messagequeue.configuration.TaskManagerProperties;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A task manager that schedules tasks which have to be executed
 */
@Service
public class TaskManager {
    private ExecutorService executorService;

    public TaskManager(TaskManagerProperties taskManagerProperties) {
        this.executorService = Executors.newFixedThreadPool(taskManagerProperties.getThreadPoolSize()); //currently no thread priority yet for processes. Will be implemented later.
    }

    public void executeTasks(List<Runnable> tasks) throws InterruptedException {
        executorService.invokeAll(
                tasks.stream()
                .map(Executors::callable)
                .toList()
        );
    }
}
