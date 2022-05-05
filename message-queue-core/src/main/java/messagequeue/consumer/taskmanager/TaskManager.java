package messagequeue.consumer.taskmanager;

import messagequeue.configuration.TaskManagerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A task manager that schedules tasks which have to be executed
 */
@Service
public class TaskManager {
    private Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private ThreadPoolExecutor threadPoolExecutor;
    private final Map<String, List<TaskPackage>> activeTaskPackagesPerConsumer = new ConcurrentHashMap<>();
    private AtomicLong totalTasksProcessed = new AtomicLong();

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

    public void executeTasks(String consumerId, List<TaskPackage> taskPackages) throws InterruptedException {
        activeTaskPackagesPerConsumer.put(consumerId, taskPackages);

        threadPoolExecutor.invokeAll(
                taskPackages.stream()
                        .map(taskPackage -> (Callable<Void>) () -> {
                            taskPackage.run();
                            totalTasksProcessed.addAndGet(taskPackage.getTotalProcessed());
                            return null;
                        })
                        .toList()
        );

        activeTaskPackagesPerConsumer.remove(consumerId, taskPackages);
    }

    public void cancelConsumerTasks(String consumerId) {
        if (activeTaskPackagesPerConsumer.containsKey(consumerId)) {
            activeTaskPackagesPerConsumer.get(consumerId).forEach(TaskPackage::cancel);
            activeTaskPackagesPerConsumer.remove(consumerId);
        } else {
            logger.warn("Can not cancel tasks for consumer '{}' as there are no active tasks in queue", consumerId);
        }
    }

    public int getTotalNumberOfConcurrentTasksForConsumer(String consumerId) {
        return activeTaskPackagesPerConsumer.get(consumerId).size();
    }

    public Map<String, Integer> getTotalNumberOfConcurrentTasksForAllConsumers() {
        return activeTaskPackagesPerConsumer
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
    }

    public int getTotalNumberOfTasksInQueue() {
        return (int) activeTaskPackagesPerConsumer
                .values()
                .stream()
                .map(
                        taskPackages -> taskPackages
                                .stream()
                                .map(TaskPackage::getRemainingTaskCount)
                                .count()
                )
                .count();
    }

    public int getTotalNumberOfTasksCurrentlyExecuting() {
        return threadPoolExecutor.getActiveCount();
    }

    public long getTotalNumberOfCompletedTasks() {
        return totalTasksProcessed.get();
    }
}
