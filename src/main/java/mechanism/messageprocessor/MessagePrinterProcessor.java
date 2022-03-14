package mechanism.messageprocessor;

import mechanism.messagebroker.MessageBrokerProxy;

public class MessagePrinterProcessor extends BaseMessageProcessor{
    public MessagePrinterProcessor(MessageBrokerProxy messageBrokerProxy, String name) {
        super(messageBrokerProxy, name);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
