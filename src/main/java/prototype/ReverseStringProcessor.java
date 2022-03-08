package prototype;

public class ReverseStringProcessor implements Subscriber, Publisher, Processor{
    private MessageBroker messageBroker;

    public ReverseStringProcessor(MessageBroker messageBroker){
        this.messageBroker = messageBroker;
        new Thread(this::consume).start();
    }
    @Override
    public void processMessage(TopicMessage message) {
        try {
            publish("reversed", new StringBuilder(message.getMessage()).reverse().toString());
            messageBroker.notifySuccessMessageProcessing(message.getTopicName(), message.getId());
        } catch (Exception ex) {
            messageBroker.notifyFailureMessageProcessing(message.getTopicName(), message.getId());
        }
    }

    @Override
    public void consume() {
        while (true) {
            TopicMessage topicMessage = messageBroker.getNextMessageForSubscriber(this);

            if (topicMessage != null) {
                processMessage(topicMessage);
            }
        }
    }

    @Override
    public void publish(String topicName, String message) {
        messageBroker.addMessage(topicName, message);
    }
}
