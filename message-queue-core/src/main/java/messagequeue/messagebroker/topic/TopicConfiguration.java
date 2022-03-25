package messagequeue.messagebroker.topic;

public class TopicConfiguration {
    private final String name;

    public TopicConfiguration(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
