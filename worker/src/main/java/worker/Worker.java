package worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * All information regarding the worker is tracked here, such as its identifier, which partition it has been assigned to.
 */
@Service
public class Worker {
    private Logger logger = LoggerFactory.getLogger(Worker.class);
    private final String identifier = UUID.randomUUID().toString();
    private int assignedPartition = -1;

    /**
     * Every instance running in the cluster has a single ConsumerManager which we can identify by the identifier
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
