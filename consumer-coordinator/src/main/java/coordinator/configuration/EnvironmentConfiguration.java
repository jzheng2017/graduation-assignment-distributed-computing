package coordinator.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:environment-${spring.profiles.active}.properties")
public class EnvironmentConfiguration {
    @Value("${partitions}")
    private int partitions;

    public int getPartitions() {
        return partitions;
    }
}
