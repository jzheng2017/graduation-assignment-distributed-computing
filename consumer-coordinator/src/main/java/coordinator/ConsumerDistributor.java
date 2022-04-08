package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ConsumerDistributor {
    private Logger logger = LoggerFactory.getLogger(ConsumerDistributor.class);
    private MessageBrokerProxy messageBrokerProxy;

    public ConsumerDistributor(MessageBrokerProxy messageBrokerProxy) {
        this.messageBrokerProxy = messageBrokerProxy;
    }

    public void addConsumer(String instanceId, ConsumerProperties consumerProperties) {
        try {
            String payload = new ObjectMapper().writeValueAsString(new AddConsumerRequest("add", consumerProperties));
            messageBrokerProxy.sendMessage(instanceId + "-consumers", payload);
            logger.info("Distributed a consumer to instance '{}'", instanceId);
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize request", e);
        }
    }

    public void removeConsumer(String instanceId, String consumerId) {
        try {
            String payload = new ObjectMapper().writeValueAsString(new RemoveConsumerRequest("remove", consumerId));
            messageBrokerProxy.sendMessage(instanceId + "-consumers", payload);
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize request", e);
        }
    }
}
