package mechanism.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:kafka.properties")
public class KafkaProperties {
    @Value("${kafka.host.url}")
    private String hostUrl;

    public String getHostUrl() {
        return hostUrl;
    }
}
