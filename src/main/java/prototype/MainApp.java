package prototype;

public class MainApp {
    public static void main(String[] args) {
        MessageBroker messageBroker = new MessageBroker();
        messageBroker.addTopic("input");
        messageBroker.addTopic("output");
        Subscriber sub1 = new EventProcessor(messageBroker);
        Subscriber sub2 = new SoutProcessor(messageBroker);
        Subscriber sub3 = new SoutProcessor(messageBroker);
        Subscriber sub4 = new SoutProcessor(messageBroker);
        messageBroker.subscribeToTopic("input", sub1);
        messageBroker.subscribeToTopic("output", sub2);
        messageBroker.subscribeToTopic("output", sub3);
        messageBroker.subscribeToTopic("output", sub4);
        //in this case these messages should be processed by sub1 (EventProcessor) that is subscribed to the input topic
        //it processes the message and then publishes to the output topic which sub2, sub3 and sub4 (SoutProcessor) is subscribed to
        //consequently these 3 will process the incoming messages in the output topic by printing to system out
        messageBroker.addMessage("input", "hello");
        messageBroker.addMessage("input", "world");
        messageBroker.addMessage("input", "this");
        messageBroker.addMessage("input", "is");
        messageBroker.addMessage("input", "message");
        messageBroker.addMessage("input", "queueing");
    }
}
