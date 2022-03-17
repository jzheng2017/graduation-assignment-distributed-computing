package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

public class MessageReverserProcessor extends BaseMessageProcessor implements Publisher {
    private Logger logger = LoggerFactory.getLogger(MessageReverserProcessor.class);

    public MessageReverserProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        super(messageBrokerProxy, name);
    }

    @Override
    public void publish(String topicName, String message) {
        messageBrokerProxy.sendMessage(topicName, message);
    }

    @Override
    public void process(String message) {
        publish("reversed", new StringBuilder(message).reverse().toString());
    }

    @RabbitListener(queues = "output", ackMode = "AUTO")
    @Override
    public void consume(String message) {
        super.consume(message);
    }

    @Override
    public void acknowledgeMessage() {

    }
}
