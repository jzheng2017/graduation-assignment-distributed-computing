package worker;

import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class Worker {
    private final static String identifier = UUID.randomUUID().toString();
    private int assignedPartition;
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
    }

    public int getAssignedPartition() {
        return assignedPartition;
    }
}
