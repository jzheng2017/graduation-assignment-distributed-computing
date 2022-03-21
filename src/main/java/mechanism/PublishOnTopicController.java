package mechanism;

import mechanism.messagebroker.MessageBrokerProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * This is just a rest endpoint for adding messages to a topic for development purposes.
 */
@RestController
public class PublishOnTopicController {
    @Autowired
    private MessageBrokerProxy messageBrokerProxy;

    @PostMapping("/publish")
    public ResponseEntity publishOnTopic(@RequestBody Message message){
        messageBrokerProxy.sendMessage(message.topicName, message.message);

        return ResponseEntity.ok().build();
    }
}
