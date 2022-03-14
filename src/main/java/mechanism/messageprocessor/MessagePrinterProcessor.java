package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

public class MessagePrinterProcessor extends BaseMessageProcessor{
    public MessagePrinterProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        super(messageBrokerProxy, name);
    }

    @RabbitListener(queues = {"reversed"})
    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
