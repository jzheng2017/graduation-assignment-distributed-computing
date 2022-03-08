package prototype;

public class MainApp {
    public static void main(String[] args) {
        MessageBroker messageBroker = new MessageBroker();
        messageBroker.addTopic("input");
        messageBroker.addTopic("reversed");
        messageBroker.addTopic("output");
        Subscriber sub1 = new EventProcessor(messageBroker);
        Subscriber sub2 = new SoutProcessor(messageBroker);
        Subscriber sub3 = new SoutProcessor(messageBroker);
        Subscriber sub4 = new SoutProcessor(messageBroker);
        Subscriber sub5 = new ReverseStringProcessor(messageBroker);
        Subscriber sub6 = new ReverseStringProcessor(messageBroker);
        messageBroker.subscribeToTopic("input", sub1);
        messageBroker.subscribeToTopic("reversed", sub2);
        messageBroker.subscribeToTopic("reversed", sub3);
        messageBroker.subscribeToTopic("reversed", sub4);
        messageBroker.subscribeToTopic("output", sub5);
        messageBroker.subscribeToTopic("output", sub6);
        //in this case these messages should be processed by sub1 (EventProcessor) that is subscribed to the input topic
        //it processes the message and then publishes to the output topic which sub5 and sub6 (ReversStringProcessor) is subscribed to
        //consequently these 2 processors will process it by reversing the string and then publishing to the 'reversed' topic
        //sub2, sub3 and sub4 (SoutProcessors) are subscribed to these topic and will process these messages by just simply printing them to system out
        //this process effectively is a chain by processing and passing on to the next topic which processors are subscribed to
        messageBroker.addMessage("input", "hello");
        messageBroker.addMessage("input", "world");
        messageBroker.addMessage("input", "this");
        messageBroker.addMessage("input", "is");
        messageBroker.addMessage("input", "message");
        messageBroker.addMessage("input", "queueing");
    }
}
