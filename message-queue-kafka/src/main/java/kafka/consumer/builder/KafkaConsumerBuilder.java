package kafka.consumer.builder;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.builder.ConsumerBuilder;
import messagequeue.consumer.builder.ConsumerConfigurationParser;
import messagequeue.consumer.builder.ConsumerFactory;
import messagequeue.messagebroker.Consumer;
import org.springframework.context.annotation.ComponentScan;
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
