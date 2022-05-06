package messagequeue.consumer.builder;

import messagequeue.consumer.Consumer;
import commons.ConsumerProperties;

/**
 * A parser interface that can parse a {@link Consumer} configuration in a given format and produce a {@link ConsumerProperties} from it
 */
public interface ConsumerConfigurationParser {
    /**
     * Create a {@link ConsumerProperties} from a consumer configuration
     * @return a consumer properties object
     */
    ConsumerProperties parse(String configuration);
}
