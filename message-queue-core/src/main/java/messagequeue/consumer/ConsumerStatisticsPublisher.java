package messagequeue.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(value = "consumer.statistics.publisher", havingValue = "on")
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

        ConsumerStatistics consumerStatistics = new ConsumerStatistics(
                consumerManager.getIdentifier(),
                totalTasksInQueue, totalTasksCompleted,
                totalTasksScheduled,
                concurrentTasksPerConsumer.entrySet().stream().map(entry -> new ConsumerTaskCount(entry.getKey(), entry.getValue())).collect(Collectors.toList()),
                Instant.now().getEpochSecond());

        String json = new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(consumerStatistics);
        messageBrokerProxy.sendMessage("consumer-statistics", json);
    }
}
