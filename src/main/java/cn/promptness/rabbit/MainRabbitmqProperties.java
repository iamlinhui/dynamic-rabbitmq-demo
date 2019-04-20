package cn.promptness.rabbit;

import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author Lynn
 * @date 2018/8/1 16:21
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.main")
public class MainRabbitmqProperties extends AbstractRabbitmqProperties {

    @Autowired(required = false)
    MessageConverter messageConverter;

    @Autowired(required = false)
    MessageRecoverer messageRecoverer;

}
