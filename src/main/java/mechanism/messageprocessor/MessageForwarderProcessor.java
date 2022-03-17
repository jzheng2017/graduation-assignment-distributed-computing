package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
public class MessageForwarderProcessor extends BaseMessageProcessor implements Publisher {
    private Logger logger = LoggerFactory.getLogger(MessageForwarderProcessor.class);

    public MessageForwarderProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        super(messageBrokerProxy, name);
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @RabbitListener(queues = "input", ackMode = "AUTO")
    @Override
    public void consume(String message) {
        process(message);
    }

    @Override
    public void acknowledgeMessage() {

    }


    @Override
    public void process(String message) {
        publish("output", message);
    }
}
