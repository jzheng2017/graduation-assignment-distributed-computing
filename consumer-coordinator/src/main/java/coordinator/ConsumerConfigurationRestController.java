package coordinator;

import commons.ConsumerProperties;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumerConfigurationRestController {
    private ConsumerCoordinator consumerCoordinator;

    public ConsumerConfigurationRestController(ConsumerCoordinator consumerCoordinator) {
        this.consumerCoordinator = consumerCoordinator;
    }

    @PostMapping("/add-consumer")
    public ResponseEntity<String> addConsumer(@RequestBody ConsumerProperties consumerConfiguration) {
        consumerCoordinator.addConsumerConfiguration(consumerConfiguration);

        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/remove-consumer/{consumerId}")
    public ResponseEntity<String> removeConsumer(@NonNull @PathVariable("consumerId") String consumerId) {
        consumerCoordinator.removeConsumerConfiguration(consumerId);

        return ResponseEntity.ok().build();
    }
}
