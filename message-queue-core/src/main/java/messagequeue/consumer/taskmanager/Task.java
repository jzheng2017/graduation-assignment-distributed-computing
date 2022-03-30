package messagequeue.consumer.taskmanager;

public record Task(String consumerId, Runnable task) {

}
