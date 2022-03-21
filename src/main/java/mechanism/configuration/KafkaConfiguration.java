package mechanism.configuration;

import mechanism.messagebroker.MessageBrokerProxy;
import mechanism.messageprocessor.ConsumerProperties;
import mechanism.messageprocessor.MessageForwarderConsumer;
import mechanism.messageprocessor.MessagePrinterConsumer;
import mechanism.messageprocessor.MessageReverserConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    public Admin admin(){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return Admin.create(props);
    }

    @Bean
    public MessagePrinterConsumer messagePrinterProcessor(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties){
        ConsumerProperties c = new ConsumerProperties("printer", "printer1", List.of("reversed"));
        return new MessagePrinterConsumer(messageBrokerProxy, kafkaProperties, c);
    }

    @Bean
    public MessageForwarderConsumer messageForwarderProcessor(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties){
        ConsumerProperties c = new ConsumerProperties("forwarder", "forwarder1", List.of("input"));
        return new MessageForwarderConsumer(messageBrokerProxy, kafkaProperties, c);
    }

    @Bean
    public MessageReverserConsumer messageReverserProcessor(MessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties){
        ConsumerProperties c = new ConsumerProperties("reverser", "reverser1", List.of("output"));
        return new MessageReverserConsumer(messageBrokerProxy, kafkaProperties, c);
    }
}
