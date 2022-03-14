package mechanism.messagebroker;

import org.springframework.amqp.core.AmqpTemplate;

public class RabbitMessageBrokerProxy extends MessageBrokerProxy {
    private AmqpTemplate template;

    public RabbitMessageBrokerProxy(AmqpTemplate amqpTemplate) {
        this.template = amqpTemplate;
    }

    @Override
    public void sendMessage(String topicName, String message) {
        template.convertAndSend(topicName, message);
    }

    @Override
    protected String getMessageFromTopic(String topicName, String subscriberName) {
        return (String)template.receiveAndConvert(topicName);
    }
}
