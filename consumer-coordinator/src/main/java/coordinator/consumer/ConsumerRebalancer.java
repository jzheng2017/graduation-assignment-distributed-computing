package coordinator.consumer;

import coordinator.ConsumerCoordinator;
import coordinator.Util;
import coordinator.dto.ConsumerTaskCount;
import coordinator.dto.WorkerStatistics;
import coordinator.worker.WorkerStatisticsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

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

        WorkerStatistics busiestInstance = workerStatistics.get(0);
        WorkerStatistics leastBusyInstance = workerStatistics.get(0);
        int busiestInstanceConcurrentTaskCount = Integer.MIN_VALUE;
        int leastBusyInstanceConcurrentTaskCount = Integer.MAX_VALUE;

        for (WorkerStatistics workerStatistic : workerStatistics) {
            int currentInstanceConcurrentTaskCount = util.getTotalConcurrentTasks(workerStatistic);
            busiestInstanceConcurrentTaskCount = util.getTotalConcurrentTasks(busiestInstance);
            leastBusyInstanceConcurrentTaskCount = util.getTotalConcurrentTasks(leastBusyInstance);

            if (currentInstanceConcurrentTaskCount > busiestInstanceConcurrentTaskCount) {
                busiestInstance = workerStatistic;
            }

            if (currentInstanceConcurrentTaskCount < leastBusyInstanceConcurrentTaskCount) {
                leastBusyInstance = workerStatistic;
            }
        }

        final int consumptionDifferenceBetweenBusiestAndLeastBusyInstance = busiestInstanceConcurrentTaskCount - leastBusyInstanceConcurrentTaskCount;

        if (consumptionDifferenceBetweenBusiestAndLeastBusyInstance >= MAX_DIFFERENCE_IN_CONSUMPTION_BEFORE_REBALANCE) {
            logger.warn("A consumer rebalance will be performed due to consumption imbalance between busiest and least busy instance, namely a difference of {} tasks", consumptionDifferenceBetweenBusiestAndLeastBusyInstance);

            if (busiestInstance.concurrentTasksPerConsumer().isEmpty()) {
                return;
            }

            ConsumerTaskCount busiestConsumer = busiestInstance.concurrentTasksPerConsumer().get(0);

            for (ConsumerTaskCount consumer : busiestInstance.concurrentTasksPerConsumer()) {
                if (consumer.count() > busiestConsumer.count()) {
                    busiestConsumer = consumer;
                }
            }

            consumerCoordinator.removeConsumerAssignment(busiestConsumer.consumerId());
        }
    }
}