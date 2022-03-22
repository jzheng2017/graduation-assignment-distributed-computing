package messagequeue.configuration;

import messagequeue.messagebroker.topic.KafkaTopicManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ApplicationStartupHandler implements ApplicationRunner {
    @Autowired
    private KafkaTopicManager kafkaTopicManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("input", 1, 1));
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("output", 1, 1));
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("reversed", 1, 1));
    }


}
