package datastorage;

import java.util.function.Supplier;

/**
 * A client that allows one to lock and unlock resources
 */
public interface LockClient {
    /**
     * Acquire lock with the passed in name
     * @param name name of the lock
     * @return
     */
    void lock(String name);

    /**
     * Release the lock with the passed in name
     * @param name name of the lock
     * @return
     */
    void unlock(String name);

    /**
     * Acquire the lock and execute the given supplier
     * @param lockName name of the lock
     * @param supplier the supplier function to execute
     * @return value that was returned from the supplier
     */
    <T> T acquireLockAndExecute(String lockName, Supplier<T> supplier);
}
