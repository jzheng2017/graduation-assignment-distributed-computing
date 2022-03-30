package messagequeue.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.messagebroker.Consumer;
import org.springframework.stereotype.Service;

@Service
public class ConsumerBuilder {
    private ConsumerConfigurationParser consumerConfigurationParser;
    private ConsumerFactory consumerFactory;

    public ConsumerBuilder(ConsumerConfigurationParser consumerConfigurationParser, ConsumerFactory consumerFactory) {
        this.consumerConfigurationParser = consumerConfigurationParser;
        this.consumerFactory = consumerFactory;
    }

    public Consumer createConsumer(String consumerConfiguration) {
        ConsumerProperties consumerProperties = consumerConfigurationParser.parse(consumerConfiguration);
        return consumerFactory.createConsumer(consumerProperties);
    }
}
