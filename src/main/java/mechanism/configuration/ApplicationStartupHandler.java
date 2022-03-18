package mechanism.configuration;

import mechanism.messagebroker.topic.KafkaTopicManager;
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
//        kafkaTopicManager.createTopic(new KafkaTopicConfiguration("test2", 1, 1));
        kafkaTopicManager.getTopics().forEach(t -> System.out.println(t.getName()));
    }


}
