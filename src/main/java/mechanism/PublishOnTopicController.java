package mechanism;

import mechanism.messagebroker.MessageBrokerProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

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
