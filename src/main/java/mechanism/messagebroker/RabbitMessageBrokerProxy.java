package mechanism.messagebroker;

import mechanism.RabbitConfiguration;
import mechanism.messagebroker.MessageBrokerProxy;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class RabbitMessageBrokerProxy extends MessageBrokerProxy {
    private AmqpTemplate template;

    public RabbitMessageBrokerProxy() {
        ApplicationContext context = new AnnotationConfigApplicationContext(RabbitConfiguration.class);
        this.template = context.getBean(AmqpTemplate.class);
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
