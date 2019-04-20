package cn.promptness.rabbit.common;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * 发送端
 *
 * @author linhuid
 * @date 2018/12/18 9:12
 * @since v1.0.0
 */
@Slf4j
@Component
public class RabbitProducer implements ConfirmCallback, ReturnCallback {

    private Map<CorrelationData, String> messageMap = Maps.newHashMap();

    private RabbitTemplate rabbitTemplate;

    @Autowired
    public RabbitProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnCallback(this);
    }

    public CorrelationData sendMessage(String exchange, String routingKey, Object content) throws AmqpException {
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        this.messageMap.put(correlationId, JSON.toJSONString(content));
        this.rabbitTemplate.convertAndSend(exchange, routingKey, content, correlationId);
        return correlationId;
    }

    public boolean confirmMessage(CorrelationData correlationData) {
        return !this.messageMap.containsKey(correlationData);
    }

    /**
     * ConfirmCallback接口用于实现消息发送到RabbitMQ交换器后接收ack回调
     * 如果消息没有到exchange,则confirm回调,ack=false
     * 如果消息到达exchange,则confirm回调,ack=true
     */
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        log.info("回调id:{} cause: {} ", correlationData, cause);
        if (ack) {
            this.messageMap.remove(correlationData);
            log.info("消息到达exchange");
        } else {
            log.error("消息没有到exchange {}", cause);
        }
    }

    /**
     * ReturnCallback接口用于实现消息发送到RabbitMQ交换器，但无相应队列与交换器绑定时的回调
     * exchange到queue成功,则不回调return
     * exchange到queue失败,则回调return(需设置mandatory=true,否则不回回调,消息就丢了)
     */
    @Override
    public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
        log.error("return--message: {} ,replyCode: {} ,replyText: {} ,exchange: {} ,routingKey: {}", new String(message.getBody()), replyCode, replyText, exchange, routingKey);
    }
}
