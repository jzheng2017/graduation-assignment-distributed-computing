package prototype;

/**
 * Just a simple processor that does some "simple" processing and publishes on another topic
 */
public class EventProcessor implements Processor, Subscriber, Publisher {
    private MessageBroker messageBroker;

    public EventProcessor(MessageBroker messageBroker) {
        this.messageBroker = messageBroker;
        new Thread(this::poll).start();
    }

    @Override
    public void processMessage(TopicMessage topicMessage) {
        try {
            // some event processing
            // ..
            // ..

            publish("output", topicMessage.getMessage());
            messageBroker.notifySuccessMessageProcessing(topicMessage.getTopicName(), topicMessage.getId());
        } catch (Exception ex) {
            messageBroker.notifyFailureMessageProcessing(topicMessage.getTopicName(), topicMessage.getId());
        }
    }

    @Override
    public void poll() {
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
