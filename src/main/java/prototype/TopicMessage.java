package prototype;

public class TopicMessage {
    private String topicName;
    private long id;
    private String message;

    public TopicMessage(String topicName, long id, String message){
        this.topicName = topicName;
        this.id = id;
        this.message = message;
    }

    public String getTopicName() {
        return topicName;
    }

    public long getId() {
        return id;
    }

    public String getMessage() {
        return message;
    }
}
