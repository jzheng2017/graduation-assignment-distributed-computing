package messagequeue.consumer;

import java.util.List;
import java.util.Map;

/**
 * The {@link ConsumerManager} is responsible for keeping track of the consumers and all other consumer-related tasks such as (un)registering consumer(s) and providing consumer information
 */
public interface ConsumerManager {
    /**
     * Construct and then register the consumer
     *
     */
    void registerConsumer(String consumerId);

    void startConsumer(String consumerId);

    /**
     * Stop and unregister a consumer
     *
     * @param consumerId the consumer id
     */
    void unregisterConsumer(String consumerId);

    void stopConsumer(String consumerId);

    /**
     * Shut down the ConsumerManager where all registered consumers will be stopped
     */
    void shutdown();

    List<String> getAllConsumers();

    /**
     * Determines whether a consumer is used internally
     * @param consumerIdentifier the identifier of the consumer
     * @return true if used internally, false otherwise
     */
    boolean isConsumerInternal(String consumerIdentifier);

    /**
     * Get the total number of running tasks of all consumers
     *
     * @return the number of running tasks
     */
    int getTotalRunningTasks();

    /**
     * Get the total number of runnings tasks of a specific consumer
     *
     * @param consumerId the consumer id
     * @return the number of running tasks
     */
    int getTotalRunningTasksForConsumer(String consumerId);

    Map<String, Integer> getTotalRunningTasksForAllConsumers();

    int getTotalNumberOfTasksInQueue();

    long getTotalNumberOfCompletedTasks();

    void unregisterAllConsumers();
}
