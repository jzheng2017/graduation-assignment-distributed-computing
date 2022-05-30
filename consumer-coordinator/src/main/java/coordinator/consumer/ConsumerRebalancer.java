package coordinator.consumer;

import commons.ConsumerTaskCount;
import commons.Util;
import commons.WorkerStatistics;
import coordinator.ConsumerCoordinator;
import coordinator.worker.WorkerStatisticsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/**
 * This class is responsible for maintaining a healthy workload balance between all workers. That is to say that one worker should not have a much higher workload than another worker.
 * This goal is achieved by periodically checking the execution statistics of the workers. If a great difference in the amount of work has been detected then a rebalance process will be started.
 * The rebalance process consists of moving one or more consumers from the worker that has a big work load to the worker that has a smaller work load. This results in a better spread out workload.
 */
@Service
@Profile(value = {"dev", "kubernetes"})
public class ConsumerRebalancer {
    private Logger logger = LoggerFactory.getLogger(ConsumerRebalancer.class);
    private static final int MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE = 10;

    private ConsumerCoordinator consumerCoordinator;
    private WorkerStatisticsReader workerStatisticsReader;
    private Util util;

    public ConsumerRebalancer(ConsumerCoordinator consumerCoordinator, WorkerStatisticsReader workerStatisticsReader, Util util) {
        this.consumerCoordinator = consumerCoordinator;
        this.workerStatisticsReader = workerStatisticsReader;
        this.util = util;
    }

    @Scheduled(fixedDelay = 5000L)
    public void reassignConsumersThatAreDisproportionatelyConsuming() {
        List<WorkerStatistics> workerStatistics = workerStatisticsReader.getAllWorkerStatistics();
        if (workerStatistics == null || workerStatistics.size() <= 1) {
            return;
        }


        workerStatistics.sort(Comparator.comparing(WorkerStatistics::totalTasksInQueue));
        WorkerStatistics leastBusyWorker = workerStatistics.get(0);
        WorkerStatistics busiestWorker = workerStatistics.get(workerStatistics.size() - 1);

        final int taskInQueueDifferenceBetweenBusiestAndLeastBusyWorker = busiestWorker.totalTasksInQueue() - leastBusyWorker.totalTasksInQueue();

        if (taskInQueueDifferenceBetweenBusiestAndLeastBusyWorker >= MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE) {

            if (busiestWorker.concurrentTasksPerConsumer().isEmpty()) {
                return;
            }

            ConsumerTaskCount busiestConsumer = getBusiestConsumerThatWillNotCauseAnotherRebalance(busiestWorker, leastBusyWorker);
            if (busiestConsumer != null) {
                logger.warn("A consumer rebalance will be performed due to consumption imbalance between busiest and least busy worker, namely a difference of {} tasks", taskInQueueDifferenceBetweenBusiestAndLeastBusyWorker);
                consumerCoordinator.removeConsumerAssignment(busiestConsumer.consumerId());
            }
        }
    }

    private ConsumerTaskCount getBusiestConsumerThatWillNotCauseAnotherRebalance(WorkerStatistics busiestWorker, WorkerStatistics leastBusyWorker) {
        ConsumerTaskCount busiestConsumer = busiestWorker.concurrentTasksPerConsumer().get(0);
        boolean consumerFound = false;
        for (ConsumerTaskCount consumer : busiestWorker.concurrentTasksPerConsumer()) {
            final boolean reassignmentWillNotCauseAnotherRebalance = (busiestWorker.totalTasksInQueue() - busiestConsumer.remainingTasksInQueue()) - (leastBusyWorker.totalTasksInQueue() + busiestConsumer.remainingTasksInQueue()) < MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE;
            if (consumer.remainingTasksInQueue() > busiestConsumer.remainingTasksInQueue() && reassignmentWillNotCauseAnotherRebalance) {
                busiestConsumer = consumer;
                consumerFound = true;
            }
        }

        return consumerFound ? busiestConsumer : null;
    }
}