package mechanism;

import mechanism.messagebroker.RabbitMessageBrokerProxy;
import mechanism.messageprocessor.MessageForwarderProcessor;
import mechanism.messageprocessor.MessagePrinterProcessor;
import mechanism.messageprocessor.MessageReverserProcessor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfiguration {

    @Bean
    public CachingConnectionFactory connectionFactory() {
        return new CachingConnectionFactory("localhost");
    }

    @Bean
    public RabbitAdmin amqpAdmin() {
        return new RabbitAdmin(connectionFactory());
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        return new RabbitTemplate(connectionFactory());
    }

    @Bean
    public MessageForwarderProcessor messageForwarderProcessor(AmqpTemplate amqpTemplate) {
        return new MessageForwarderProcessor(new RabbitMessageBrokerProxy(amqpTemplate), "fp");
    }

    @Bean
    public MessageReverserProcessor messageReverserProcessor(AmqpTemplate amqpTemplate) {
        return new MessageReverserProcessor(new RabbitMessageBrokerProxy(amqpTemplate), "rp");
    }

    @Bean
    public MessagePrinterProcessor messagePrinterProcessor(AmqpTemplate amqpTemplate) {
        return new MessagePrinterProcessor(new RabbitMessageBrokerProxy(amqpTemplate), "pp");
    }

    @Bean
    public Queue inputQueue() {
        return new Queue("input");
    }
    @Bean
    public Queue reversedQueue() {
        return new Queue("reversed");
    }
    @Bean
    public Queue outputQueue() {
        return new Queue("output");
    }
}
