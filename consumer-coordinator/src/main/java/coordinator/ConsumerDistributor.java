package coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import messagequeue.consumer.ConsumerProperties;
import messagequeue.messagebroker.MessageBrokerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

@Service
public class ConsumerDistributor {
    private Logger logger = LoggerFactory.getLogger(ConsumerDistributor.class);
    private Queue<String> consumerRequestsToDistribute = new LinkedList<>();
    private MessageBrokerProxy messageBrokerProxy;

    public ConsumerDistributor(MessageBrokerProxy messageBrokerProxy) {
        this.messageBrokerProxy = messageBrokerProxy;
        try {
            consumerRequestsToDistribute.add(new ObjectMapper().writeValueAsString(new ConsumerRequest("add", new ConsumerProperties("uppercase", "uppercase1", Set.of("input")))));
            consumerRequestsToDistribute.add(new ObjectMapper().writeValueAsString(new ConsumerRequest("add", new ConsumerProperties("reverser", "reverser1", Set.of("output")))));
            consumerRequestsToDistribute.add(new ObjectMapper().writeValueAsString(new ConsumerRequest("add", new ConsumerProperties("printer", "printer1", Set.of("reversed")))));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public void distribute(Set<String> instances) {
        String[] instancesArray = new String[instances.size()];
        instances.toArray(instancesArray);
        Random random = new Random();
        while (!consumerRequestsToDistribute.isEmpty()) {
            if (instancesArray.length > 0) {
                int num = random.nextInt(instances.size());
                String instanceId = instancesArray[num];
                messageBrokerProxy.sendMessage(instanceId + "-consumers", consumerRequestsToDistribute.poll());
                logger.info("Distributed a consumer to instance '{}'", instanceId);
            } else {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
