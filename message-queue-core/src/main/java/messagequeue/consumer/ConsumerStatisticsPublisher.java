package messagequeue.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ConsumerStatisticsPublisher {
    private MessageBrokerProxy messageBrokerProxy;
    private ConsumerManager consumerManager;

    public ConsumerStatisticsPublisher(MessageBrokerProxy messageBrokerProxy, ConsumerManager consumerManager) {
        this.messageBrokerProxy = messageBrokerProxy;
        this.consumerManager = consumerManager;
    }

    @Scheduled(fixedRate = 5000L)
    public void publishStatistic() throws JsonProcessingException {
        Map<String, Integer> concurrentTasksPerConsumer = consumerManager.getTotalRunningTasksForAllConsumers();
        long totalTasksCompleted = consumerManager.getTotalNumberOfCompletedTasks();
        int totalTasksInQueue = consumerManager.getTotalNumberOfTasksInQueue();
        long totalTasksScheduled = consumerManager.getTotalNumberOfTasksScheduled();

        ConsumerStatistics consumerStatistics = new ConsumerStatistics(consumerManager.getIdentifier(),totalTasksInQueue, totalTasksCompleted, totalTasksScheduled, concurrentTasksPerConsumer);
        String json = new ObjectMapper().writeValueAsString(consumerStatistics);
        messageBrokerProxy.sendMessage("consumer-statistics", json);
    }
}