package coordinator.worker;

import coordinator.dto.WorkerStatistics;
import datastorage.KVClient;
import datastorage.configuration.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class WorkerStatisticsReader {
    private Logger logger = LoggerFactory.getLogger(WorkerStatisticsReader.class);
    private KVClient kvClient;
    private WorkerStatisticsDeserializer workerStatisticsDeserializer;
    public WorkerStatisticsReader(KVClient kvClient, WorkerStatisticsDeserializer workerStatisticsDeserializer) {
        this.kvClient = kvClient;
        this.workerStatisticsDeserializer = workerStatisticsDeserializer;
    }

    public WorkerStatistics getWorkerStatistics(String workerId) {
        final String key = KeyPrefix.WORKER_STATISTICS + "-" + workerId;
        try {
            String workerStatisticsSerialized = kvClient.get(key).thenApply(getResponse -> getResponse.keyValues().get(key)).get();
            return workerStatisticsDeserializer.deserialize(workerStatisticsSerialized);
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not retrieve the worker statistics for worker '{}'", workerId, e);
            return null;
        }
    }
}