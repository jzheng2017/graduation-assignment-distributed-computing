package messagequeue.consumer;

import messagequeue.messagebroker.Consumer;

import java.util.List;

/**
 * The {@link ConsumerManager} is responsible for keeping track of the consumers and all other consumer-related tasks such as (un)registering consumer(s) and providing consumer information
 */
public interface ConsumerManager {
    /**
     * Construct and then register the consumer based on the passed in consumer configuration
     *
     * @param consumerConfiguration a consumer configuration
     */
    void registerConsumer(String consumerConfiguration);

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

    List<Consumer> getAllConsumers();

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

    int getTotalNumberOfTasksInQueue();

    long getTotalNumberOfCompletedTasks();

    long getTotalNumberOfTasksScheduled();

    void unregisterAllConsumers();
}
