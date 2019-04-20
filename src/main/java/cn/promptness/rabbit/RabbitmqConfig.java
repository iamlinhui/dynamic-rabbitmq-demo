package cn.promptness.rabbit;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

/**
 * @author Lynn
 * @date 2018/8/1 19:17
 */
@Configuration
public class RabbitmqConfig {

    @Autowired
    MainRabbitmqProperties mainRabbitmqProperties;

    @Autowired
    DamaiRabbitmqProperties damaiRabbitmqProperties;


    private ConnectionFactory damaiRabbitmqConnectionFactory() throws Exception {

        RabbitConnectionFactoryBean rabbitConnectionFactoryBean = new RabbitConnectionFactoryBean();

        ConnectionFactory connectionFactory = damaiRabbitmqProperties.config(rabbitConnectionFactoryBean);

        return connectionFactory;
    }

    private ConnectionFactory mainRabbitmqConnectionFactory() throws Exception {

        RabbitConnectionFactoryBean rabbitConnectionFactoryBean = new RabbitConnectionFactoryBean();

        ConnectionFactory connectionFactory = mainRabbitmqProperties.config(rabbitConnectionFactoryBean);

        return connectionFactory;
    }


    @Bean
    public SimpleRoutingConnectionFactory simpleRoutingConnectionFactory() throws Exception {

        ConnectionFactory mainRabbitmqConnectionFactory = mainRabbitmqConnectionFactory();

        ConnectionFactory damaiRabbitmqConnectionFactory = damaiRabbitmqConnectionFactory();


        SimpleRoutingConnectionFactory simpleRoutingConnectionFactory = new SimpleRoutingConnectionFactory();

        HashMap<Object, ConnectionFactory> hashMap = new HashMap<>(2);

        hashMap.put(RabbitmqConnectionEnum.BASIC, mainRabbitmqConnectionFactory);

        hashMap.put(RabbitmqConnectionEnum.BUSINESS, damaiRabbitmqConnectionFactory);

        //装配连接集合
        simpleRoutingConnectionFactory.setTargetConnectionFactories(hashMap);

        //设置默认连接
        simpleRoutingConnectionFactory.setDefaultTargetConnectionFactory(mainRabbitmqConnectionFactory);

        return simpleRoutingConnectionFactory;

    }

    /**
     * 配置注解监听时所需要的连接</br>
     * 根据不同的containerFactory来监听不同的rabbitmq实例</br>
     * #@RabbitListener(queues = {"hello"}, containerFactory = "rabbitListenerContainerFactory")</br>
     * 若有多个不同实例的监听，则需要配置多个rabbitListenerContainerFactory，然后监听时指定containerFactory</br>
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(SimpleRoutingConnectionFactory connectionFactory) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        ConnectionFactory targetConnectionFactory = connectionFactory.getTargetConnectionFactory(RabbitmqConnectionEnum.BASIC);
        mainRabbitmqProperties.configure(factory, targetConnectionFactory);

        return factory;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListener2ContainerFactory(SimpleRoutingConnectionFactory connectionFactory) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        ConnectionFactory targetConnectionFactory = connectionFactory.getTargetConnectionFactory(RabbitmqConnectionEnum.BUSINESS);
        damaiRabbitmqProperties.configure(factory, targetConnectionFactory);

        return factory;
    }

}
