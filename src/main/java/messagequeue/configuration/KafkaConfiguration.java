package messagequeue.configuration;

import messagequeue.consumer.ConsumerProperties;
import messagequeue.consumer.MessageForwarderConsumer;
import messagequeue.consumer.MessagePrinterConsumer;
import messagequeue.consumer.MessageReverserConsumer;
import messagequeue.consumer.taskmanager.TaskManager;
import messagequeue.messagebroker.KafkaMessageBrokerProxy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.Set;

@Configuration
public class KafkaConfiguration {

    @Bean
    public Admin admin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return Admin.create(props);
    }

//    @Bean
//    public MessagePrinterConsumer messagePrinterProcessor(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager) {
//        ConsumerProperties c = new ConsumerProperties("printer", "printer1", Set.of("reversed"));
//        return new MessagePrinterConsumer(messageBrokerProxy, kafkaProperties, c, taskManager);
//    }
//
//    @Bean
//    public MessageForwarderConsumer messageForwarderProcessor(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager) {
//        ConsumerProperties c = new ConsumerProperties("forwarder", "forwarder1", Set.of("input"));
//        return new MessageForwarderConsumer(messageBrokerProxy, kafkaProperties, c, taskManager);
//    }
//
//    @Bean
//    public MessageReverserConsumer messageReverserProcessor(KafkaMessageBrokerProxy messageBrokerProxy, KafkaProperties kafkaProperties, TaskManager taskManager) {
//        ConsumerProperties c = new ConsumerProperties("reverser", "reverser1", Set.of("output"));
//        return new MessageReverserConsumer(messageBrokerProxy, kafkaProperties, c, taskManager);
//    }
}
