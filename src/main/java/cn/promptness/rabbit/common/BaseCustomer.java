package cn.promptness.rabbit.common;

import cn.promptness.rabbit.ServiceException;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;

/**
 * 消费端
 *
 * @author linhuid
 * @date 2018/12/17 14:37
 * @since v1.0.0
 */
@Slf4j
public abstract class BaseCustomer<T> {

    private Class<T> clazz;

    /**
     * 重试次数
     */
    private final static int RETRY_COUNT = 3;
    /**
     * 重试时间间隔
     */
    private final static int RETRY_COUNT_LIMIT_TIME = 30000;

    @SuppressWarnings("unchecked")
    public BaseCustomer() {
        Type type = this.getClass().getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) type;
        this.clazz = (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    /**
     * 消息回调
     *
     * @param message 消息
     * @param channel 通道
     */
    @RabbitHandler
    public void callback(T vo, Message message, Channel channel) throws Exception {
        final long tagId = message.getMessageProperties().getDeliveryTag();
        try {
            vo = JSONObject.parseObject(message.getBody(), clazz);
        } catch (Exception e) {
            log.error("接收消息失败,错误信息{}", e.getMessage());
            channel.basicNack(tagId, false, false);
        }

        final RecoveryCallback<Object> recoveryCallback = getRecoveryCallback(channel, tagId);

        final RetryCallback<Object, Exception> retryCallback = getExceptionRetryCallback(channel, tagId, vo);

        getRetryTemplate().execute(retryCallback, recoveryCallback);

    }


    /**
     * 执行逻辑
     *
     * @param message 消息实体
     * @param channel 管道
     */
    protected void doCustomer(T message, Channel channel, long tagId) throws IOException {
        try {
            log.info("消费端开始处理数据....");
            doCustomer(message);
            channel.basicAck(tagId, false);
            log.info("消费端开始处理结束");
        } catch (ServiceException s) {
            log.error("消费端操作记录同步失败,错误信息{}", s.getMessage());
            throw s;
            // 这个点特别注意，重试的根源通过Exception返回
        } catch (Exception e) {
            log.error("消费端操作记录同步失败,错误信息{}", e.getMessage());
        }
    }

    /**
     * 执行逻辑
     *
     * @param message 消息实体
     */
    public abstract void doCustomer(T message);


    /**
     * 通过RecoveryCallback 重试流程正常结束或者达到重试上限后的退出恢复操作实例
     *
     * @param channel 消息管道
     * @param tagId   消息标签
     * @return org.springframework.retry.RecoveryCallback<java.lang.Object>
     * @author linhuid
     * @date 2019/3/9 15:31
     * @since v1.0.0
     */
    private RecoveryCallback<Object> getRecoveryCallback(final Channel channel, final long tagId) {

        return new RecoveryCallback<Object>() {
            @Override
            public Object recover(RetryContext context) throws Exception {
                channel.basicNack(tagId, false, false);
                return null;
            }
        };
    }

    /***
     * 构建重试模板实例
     *
     * @author linhuid
     * @date 2019/3/2 9:14
     * @return org.springframework.retry.support.RetryTemplate
     * @since v1.0.0
     */
    private RetryTemplate getRetryTemplate() {
        //
        RetryTemplate retryTemplate = new RetryTemplate();
        // 设置重试策略，主要设置重试次数
        SimpleRetryPolicy policy = new SimpleRetryPolicy(RETRY_COUNT, Collections.singletonMap(Exception.class, true));
        // 设置重试回退操作策略，主要设置重试间隔时间
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(RETRY_COUNT_LIMIT_TIME);
        retryTemplate.setRetryPolicy(policy);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    /**
     * 通过RetryCallback 重试回调实例包装正常逻辑逻辑，
     * 第一次执行和重试执行执行的都是这段逻辑 RetryContext 重试操作上下文约定，统一spring-try包装
     *
     * @param channel 消息通道
     * @param tagId   消息标签
     * @param vo      消息实体
     * @return org.springframework.retry.RetryCallback<java.lang.Object, java.lang.Exception>
     * @author linhuid
     * @date 2019/3/9 15:27
     * @since v1.0.0
     */
    private RetryCallback<Object, Exception> getExceptionRetryCallback(Channel channel, long tagId, T vo) {
        return new RetryCallback<Object, Exception>() {
            @Override
            public Object doWithRetry(RetryContext context) throws Exception {
                doCustomer(vo, channel, tagId);
                return null;
            }
        };
    }
}
