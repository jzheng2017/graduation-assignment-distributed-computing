package coordinator;

/**
 * The status of the consumer. It is used to determine whether the consumer has already been assigned to a partition. {@link ConsumerStatus#ASSIGNED} means that is has been assigned to a partition and {@link ConsumerStatus#UNASSIGNED} otherwise.
 */
public enum ConsumerStatus {
    ASSIGNED,
    UNASSIGNED
}
