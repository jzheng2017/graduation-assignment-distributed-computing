package datastorage.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Profile({"dev", "kubernetes"})
@PropertySource("classpath:etcd-${spring.profiles.active}.properties")
public class EtcdProperties {
    @Value("${etcd.base.url}")
    private String baseUrl;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}
