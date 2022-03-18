package mechanism.configuration;

import mechanism.messagebroker.KafkaMessageBrokerProxy;
import mechanism.messageprocessor.ConsumerProperties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
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
    public ConsumerProperties consumerProperties(){
        return new ConsumerProperties("pp", "pp1", List.of("test1"));
    }
}
