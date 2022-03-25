package messagequeue.consumer.taskmanager;

import messagequeue.configuration.TaskManagerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A task manager that schedules tasks which have to be executed
 */
@ComponentScan(basePackages = {"messagequeue.consumer.taskmanager"})
@Service
public class TaskManager {
    private Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private ThreadPoolExecutor threadPoolExecutor;

    public TaskManager(TaskManagerProperties taskManagerProperties) {
        final int corePoolSize = taskManagerProperties.getThreadPoolSize();
        final int maxPoolSize = taskManagerProperties.getThreadPoolSize();
        //currently no thread priority yet for tasks. Will be implemented later.
        this.threadPoolExecutor = new ThreadPoolExecutor(
                taskManagerProperties.getThreadPoolSize(),
                taskManagerProperties.getThreadPoolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()
        );

        logger.info("ThreadPoolExecutor created with core pool size {}, max pool size {} and {} as queue implementation", corePoolSize, maxPoolSize, threadPoolExecutor.getQueue().getClass().getName());
    }

    public void executeTasks(List<Runnable> tasks) throws InterruptedException {
        threadPoolExecutor.invokeAll(
                tasks.stream()
                        .map(Executors::callable)
                        .toList()
        );
    }

    public int getTotalNumberOfTasksInQueue() {
        return threadPoolExecutor.getQueue().size();
    }

    public int getTotalNumberOfTasksCurrentlyExecuting() {
        return threadPoolExecutor.getActiveCount();
    }

    public long getTotalNumberOfCompletedTasks() {
        return threadPoolExecutor.getCompletedTaskCount();
    }

    public long getTotalNumberOfTasksScheduled() {
        return threadPoolExecutor.getTaskCount();
    }
}
