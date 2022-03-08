package prototype;

/**
 * A processor that processes the message by just printing to system out.
 */
public class SoutProcessor implements Processor, Subscriber{

    private MessageBroker messageBroker;

    public SoutProcessor(MessageBroker messageBroker) {
        this.messageBroker = messageBroker;
        new Thread(this::poll).start();
    }

    @Override
    public void processMessage(TopicMessage message) {
        try {
            System.out.println(message.getMessage());
            messageBroker.notifySuccessMessageProcessing(message.getTopicName(), message.getId());
        } catch (Exception ex) {
            messageBroker.notifyFailureMessageProcessing(message.getTopicName(), message.getId());
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
}
