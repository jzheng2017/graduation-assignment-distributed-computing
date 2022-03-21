package messagequeue.configuration;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:kafka.properties")
public class KafkaProperties {
    @Value("${kafka.host.url}")
    private String hostUrl;

    private String keyDeserializer = StringDeserializer.class.getName();
    private String valueDeserializer = StringDeserializer.class.getName();

    public String getHostUrl() {
        return hostUrl;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }
}
