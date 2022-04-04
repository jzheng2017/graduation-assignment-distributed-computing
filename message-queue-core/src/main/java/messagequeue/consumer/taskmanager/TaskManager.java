package messagequeue.consumer.taskmanager;

import messagequeue.configuration.TaskManagerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A task manager that schedules tasks which have to be executed
 */
@Service
@Import(TaskManagerProperties.class)
public class TaskManager {
    private Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private ThreadPoolExecutor threadPoolExecutor;
    private final Map<String, AtomicInteger> numberOfConcurrentTasksPerConsumer = new ConcurrentHashMap<>();

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

    public void executeTasks(List<Task> tasks) throws InterruptedException {
        threadPoolExecutor.invokeAll(
                tasks.stream()
                        .map(task -> (Callable<Void>) () -> {
                            AtomicInteger concurrentTasksOfConsumer;

                            synchronized (numberOfConcurrentTasksPerConsumer) {
                                concurrentTasksOfConsumer = numberOfConcurrentTasksPerConsumer.get(task.consumerId());
                                if (concurrentTasksOfConsumer == null) {
                                    concurrentTasksOfConsumer = new AtomicInteger();
                                    numberOfConcurrentTasksPerConsumer.put(task.consumerId(), concurrentTasksOfConsumer);
                                }
                            }

                            concurrentTasksOfConsumer.incrementAndGet();
                            task.task().run();
                            concurrentTasksOfConsumer.decrementAndGet();

                            return null;
                        })
                        .toList()
        );
    }

    public int getTotalNumberOfConcurrentTasksForConsumer(String consumerId) {
        AtomicInteger concurrentTasks = numberOfConcurrentTasksPerConsumer.get(consumerId);

        if (concurrentTasks != null) {
            return concurrentTasks.get();
        }

        return 0;
    }

    public Map<String, Integer> getTotalNumberOfConcurrentTasksForAllConsumers() {
        Map<String, Integer> totalConcurrentTasksAllConsumers = new HashMap<>();

        for (Map.Entry<String, AtomicInteger> consumerEntry : numberOfConcurrentTasksPerConsumer.entrySet()) {
            totalConcurrentTasksAllConsumers.put(consumerEntry.getKey(), consumerEntry.getValue().get());
        }

        return totalConcurrentTasksAllConsumers;
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
