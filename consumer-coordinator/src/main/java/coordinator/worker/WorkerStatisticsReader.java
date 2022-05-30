package coordinator.worker;

import commons.KeyPrefix;
import commons.WorkerStatistics;
import datastorage.KVClient;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
            Thread.currentThread().interrupt();
            logger.warn("Could not retrieve the worker statistics for worker '{}' correctly", workerId, e);
            return null;
        }
    }

    public List<WorkerStatistics> getAllWorkerStatistics() {
        try {
            return kvClient
                    .getByPrefix(KeyPrefix.WORKER_STATISTICS)
                    .get()
                    .keyValues()
                    .values()
                    .stream()
                    .map(workerStatisticsDeserializer::deserialize)
                    .collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Could not retrieve the worker statistics correctly", e);
            return Collections.emptyList();
        }
    }
}
