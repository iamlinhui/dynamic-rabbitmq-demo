package cn.promptness.rabbit.demo;

import cn.promptness.rabbit.common.BaseCustomer;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @Author : Lynn
 * @Date : 2018-04-21 19:04
 */
//@RabbitListener(
//        bindings = @QueueBinding(
//                value = @Queue(value = "hello.message", durable = "true"),
//                exchange = @Exchange(value = "spring.boot.message", durable = "true"),
//                key = "hello.key"
//        )
//)
@RabbitListener(queues = "hello.message", containerFactory = "rabbitListenerContainerFactory")
@Component
public class HelloCustomer extends BaseCustomer<String> {


    @Override
    public void doCustomer(String message) {

    }
}
