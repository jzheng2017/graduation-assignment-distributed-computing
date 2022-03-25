package kafka.configuration;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
@Configuration
@PropertySource("classpath:kafka.properties")
public class KafkaProperties {
    @Value("${kafka.host.url}")
    private String hostUrl;

    private final String keyDeserializer = StringDeserializer.class.getName();
    private final String valueDeserializer = StringDeserializer.class.getName();
    private final String keySerializer = StringSerializer.class.getName();
    private final String valueSerializer = StringSerializer.class.getName();

    public String getHostUrl() {
        return hostUrl;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }
}
