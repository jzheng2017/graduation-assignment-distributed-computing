package worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * All information regarding the worker is tracked here, such as its identifier, which partition it has been assigned to.
 *
 * Workers can be seen as containers that run consumers. So each pod is a worker (pod).
 * A worker is then responsible for a single partition (not to be confused with topic partitions).
 * Consumers are assigned to this partition, when it is assigned to it then the worker that is responsible for this partition has to start and run the consumer.
 */
@Service
public class Worker {
    private Logger logger = LoggerFactory.getLogger(Worker.class);
    private final String identifier = UUID.randomUUID().toString();
    private int assignedPartition = -1;

    /**
     * Every worker running in the cluster has an identifier which is used to distinguish between the various workers
     *
     * @return identifier
     */
    public String getIdentifier() {
        return identifier;
    }

    public void setAssignedPartition(int assignedPartition) {
        this.assignedPartition = assignedPartition;
        logger.info("New partition assigned to worker: {}", assignedPartition);
    }

    public int getAssignedPartition() {
        return assignedPartition;
    }
}
