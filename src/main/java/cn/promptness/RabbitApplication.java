package cn.promptness;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import java.io.IOException;

/**
 * @author linhui
 */
@SpringBootApplication
@Import(Jackson2JsonMessageConverter.class)
@Slf4j
public class RabbitApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitApplication.class, args);

    }

    //@RabbitListener(bindings = @QueueBinding(value = @Queue(value = "hello", durable = "true"), exchange = @Exchange(value = "spring-boot-exchange", durable = "true"), key = "hello"))

    @RabbitListener(queues = {"hello"}, containerFactory = "rabbitListenerContainerFactory")
    public void receiveSendEmailMessage(Channel channel, Message message) throws IOException {
        log.info(channel.getConnection().getAddress().toString());
        String body = new String(message.getBody());
        log.info(body);

        //确认消息
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = {"hello"}, containerFactory = "rabbitListener2ContainerFactory")
    public void receiveBaseSendEmailMessage(Channel channel, Message message) throws IOException {
        log.info(channel.getConnection().getAddress().toString());
        String body = new String(message.getBody());
        log.info(body);
        //确认消息
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }


    @RabbitListeners({@RabbitListener(queues = {"hello"}, containerFactory = "rabbitListener2ContainerFactory"), @RabbitListener(queues = {"hello"}, containerFactory = "rabbitListenerContainerFactory")})
    public void receiveBothSendEmailMessage(Channel channel, Message message) throws IOException {
        log.info("rabbitListenerBothContainerFactory");
        log.info(channel.getConnection().getAddress().toString());
        String body = new String(message.getBody());
        log.info(body);
        //确认消息
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }


}
