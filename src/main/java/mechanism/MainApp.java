package mechanism;

import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messagebroker.RabbitMessageBrokerProxy;
import mechanism.messageprocessor.MessageForwarderProcessor;
import mechanism.messageprocessor.MessagePrinterProcessor;
import mechanism.messageprocessor.MessageProcessor;
import mechanism.messageprocessor.MessageReverserProcessor;

public class MainApp {
    public static void main(String[] args) {
        MessageBrokerProxy messageBrokerProxy = new RabbitMessageBrokerProxy();
        MessageProcessor fp = new MessageForwarderProcessor(messageBrokerProxy, "fp");
        MessageProcessor pp = new MessagePrinterProcessor(messageBrokerProxy, "pp");
        MessageProcessor rp = new MessageReverserProcessor(messageBrokerProxy, "rp");
        fp.subscribe("input");
        rp.subscribe("output");
        pp.subscribe("reversed");

        messageBrokerProxy.sendMessage("input", "hello");
    }
}
