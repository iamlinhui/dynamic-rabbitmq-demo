package cn.promptness.rabbit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.ValueExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.expression.Expression;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitApplicationTests {


    @Autowired
    RabbitTemplate rabbitTemplate;

    @Test
    public void contextLoads() {

        //根据key切换发送消息时的连接
        rabbitTemplate.setSendConnectionFactorySelectorExpression(new ValueExpression(RabbitmqConnectionEnum.BUSINESS) );

        String exchange = "",  routingKey = "hello",  content = "Hello DynamicRabbitmq";
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(exchange, routingKey, content, correlationId);
    }



}
