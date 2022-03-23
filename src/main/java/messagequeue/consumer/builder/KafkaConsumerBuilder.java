package messagequeue.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.messagebroker.Consumer;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerBuilder implements ConsumerBuilder {
    private ConsumerConfigurationParser consumerConfigurationParser;
    private ConsumerFactory consumerFactory;

    public KafkaConsumerBuilder(ConsumerConfigurationParser consumerConfigurationParser, ConsumerFactory consumerFactory) {
        this.consumerConfigurationParser = consumerConfigurationParser;
        this.consumerFactory = consumerFactory;
    }

    @Override
    public Consumer createConsumer(String consumerConfiguration) {
        ConsumerProperties consumerProperties = consumerConfigurationParser.parse(consumerConfiguration);
        return consumerFactory.createConsumer(consumerProperties);
    }
}
