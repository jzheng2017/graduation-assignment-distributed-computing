package prototype;

public class MainApp {
    public static void main(String[] args) {
        MessageBroker messageBroker = new MessageBroker();
        messageBroker.addTopic("input");
        messageBroker.addTopic("output");
        Subscriber sub1 = new EventProcessor(messageBroker);
        Subscriber sub2 = new SoutProcessor(messageBroker);
        messageBroker.subscribeToTopic("input", sub1);
        messageBroker.subscribeToTopic("output", sub2);
        messageBroker.addMessage("input", "hello");
        messageBroker.addMessage("input", "world");
        messageBroker.addMessage("input", "this");
        messageBroker.addMessage("input", "is");
        messageBroker.addMessage("input", "message");
        messageBroker.addMessage("input", "queueing");
    }
}
