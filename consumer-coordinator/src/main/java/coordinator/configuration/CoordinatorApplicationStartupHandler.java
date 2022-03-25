package coordinator.configuration;

import kafka.configuration.KafkaProperties;
import kafka.consumer.builder.KafkaConsumerBuilder;
import kafka.messagebroker.KafkaMessageBrokerProxy;
import messagequeue.consumer.ConsumerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Import(value = {KafkaMessageBrokerProxy.class, KafkaProperties.class})
@Component
public class CoordinatorApplicationStartupHandler implements ApplicationRunner {
    @Autowired
    private KafkaMessageBrokerProxy kafkaMessageBrokerProxy;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("test");
        kafkaMessageBrokerProxy.sendMessage("input", "vrijdag2");
    }
}
