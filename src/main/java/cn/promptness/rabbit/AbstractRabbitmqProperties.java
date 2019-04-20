package cn.promptness.rabbit;


import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.*;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn
 * @date 2018/8/1 16:56
 */
@Data
public abstract class AbstractRabbitmqProperties {

    /**
     * RabbitMQ host.
     */
    private String host = "localhost";

    /**
     * RabbitMQ port.
     */
    private int port = 5672;

    /**
     * Login user to authenticate to the broker.
     */
    private String username;

    /**
     * Login to authenticate against the broker.
     */
    private String password;

    /**
     * SSL configuration.
     */
    private final Ssl ssl = new Ssl();

    /**
     * Virtual host to use when connecting to the broker.
     */
    private String virtualHost;

    /**
     * Comma-separated list of addresses to which the client should connect.
     */
    private String addresses;

    /**
     * Requested heartbeat timeout, in seconds; zero for none.
     */
    private Integer requestedHeartbeat;

    /**
     * Enable publisher confirms.
     */
    private boolean publisherConfirms;

    /**
     * Enable publisher returns.
     */
    private boolean publisherReturns;

    /**
     * Connection timeout, in milliseconds; zero for infinite.
     */
    private Integer connectionTimeout;

    /**
     * Cache configuration.
     */
    private final Cache cache = new Cache();

    /**
     * Listener container configuration.
     */
    private final Listener listener = new Listener();

    private final Template template = new Template();

    private List<Address> parsedAddresses;

    private MessageRecoverer messageRecoverer;

    private MessageConverter messageConverter;

    /**
     * 装配连接工厂
     *
     * @param factory
     * @return
     * @throws Exception
     */
    public ConnectionFactory config(RabbitConnectionFactoryBean factory) throws Exception {

        if (this.determineHost() != null) {
            factory.setHost(this.determineHost());
        }
        factory.setPort(this.determinePort());
        if (this.determineUsername() != null) {
            factory.setUsername(this.determineUsername());
        }
        if (this.determinePassword() != null) {
            factory.setPassword(this.determinePassword());
        }
        if (this.determineVirtualHost() != null) {
            factory.setVirtualHost(this.determineVirtualHost());
        }
        if (this.getRequestedHeartbeat() != null) {
            factory.setRequestedHeartbeat(this.getRequestedHeartbeat());
        }
        Ssl ssl = this.getSsl();
        if (ssl.isEnabled()) {
            factory.setUseSSL(true);
            if (ssl.getAlgorithm() != null) {
                factory.setSslAlgorithm(ssl.getAlgorithm());
            }
            factory.setKeyStore(ssl.getKeyStore());
            factory.setKeyStorePassphrase(ssl.getKeyStorePassword());
            factory.setTrustStore(ssl.getTrustStore());
            factory.setTrustStorePassphrase(ssl.getTrustStorePassword());
        }
        if (this.getConnectionTimeout() != null) {
            factory.setConnectionTimeout(this.getConnectionTimeout());
        }
        factory.afterPropertiesSet();
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(factory.getObject());
        connectionFactory.setAddresses(this.determineAddresses());
        connectionFactory.setPublisherConfirms(this.isPublisherConfirms());
        connectionFactory.setPublisherReturns(this.isPublisherReturns());
        if (this.getCache().getChannel().getSize() != null) {
            connectionFactory.setChannelCacheSize(this.getCache().getChannel().getSize());
        }
        if (this.getCache().getConnection().getMode() != null) {
            connectionFactory.setCacheMode(this.getCache().getConnection().getMode());
        }
        if (this.getCache().getConnection().getSize() != null) {
            connectionFactory.setConnectionCacheSize(this.getCache().getConnection().getSize());
        }
        if (this.getCache().getChannel().getCheckoutTimeout() != null) {
            connectionFactory.setChannelCheckoutTimeout(this.getCache().getChannel().getCheckoutTimeout());
        }
        return connectionFactory;

    }

    /**
     * 监听消息的连接
     *
     * @param factory
     * @param connectionFactory
     */
    public void configure(SimpleRabbitListenerContainerFactory factory,ConnectionFactory connectionFactory) {
        Assert.notNull(factory, "Factory must not be null");
        Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
        factory.setConnectionFactory(connectionFactory);
        if (this.messageConverter != null) {
            factory.setMessageConverter(this.messageConverter);
        }
        AmqpContainer config = this.getListener().getSimple();
        factory.setAutoStartup(config.isAutoStartup());
        if (config.getAcknowledgeMode() != null) {
            factory.setAcknowledgeMode(config.getAcknowledgeMode());
        }
        if (config.getConcurrency() != null) {
            factory.setConcurrentConsumers(config.getConcurrency());
        }
        if (config.getMaxConcurrency() != null) {
            factory.setMaxConcurrentConsumers(config.getMaxConcurrency());
        }
        if (config.getPrefetch() != null) {
            factory.setPrefetchCount(config.getPrefetch());
        }
        if (config.getTransactionSize() != null) {
            factory.setTxSize(config.getTransactionSize());
        }
        if (config.getDefaultRequeueRejected() != null) {
            factory.setDefaultRequeueRejected(config.getDefaultRequeueRejected());
        }
        if (config.getIdleEventInterval() != null) {
            factory.setIdleEventInterval(config.getIdleEventInterval());
        }
        ListenerRetry retryConfig = config.getRetry();
        if (retryConfig.isEnabled()) {
            RetryInterceptorBuilder<?> builder = (retryConfig.isStateless()
                    ? RetryInterceptorBuilder.stateless()
                    : RetryInterceptorBuilder.stateful());
            builder.maxAttempts(retryConfig.getMaxAttempts());
            builder.backOffOptions(retryConfig.getInitialInterval(),retryConfig.getMultiplier(), retryConfig.getMaxInterval());
            MessageRecoverer recoverer = (this.messageRecoverer != null ? this.messageRecoverer : new RejectAndDontRequeueRecoverer());
            builder.recoverer(recoverer);
            factory.setAdviceChain(builder.build());
        }

    }


    /**
     * Returns the host from the first address, or the configured host if no addresses
     * have been set.
     *
     * @return the host
     * @see #setAddresses(String)
     * @see #getHost()
     */
    public String determineHost() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return getHost();
        }
        return this.parsedAddresses.get(0).host;
    }

    /**
     * Returns the port from the first address, or the configured port if no addresses
     * have been set.
     *
     * @return the port
     * @see #setAddresses(String)
     * @see #getPort()
     */
    public int determinePort() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return getPort();
        }
        Address address = this.parsedAddresses.get(0);
        return address.port;
    }

    /**
     * Returns the comma-separated addresses or a single address ({@code host:port})
     * created from the configured host and port if no addresses have been set.
     *
     * @return the addresses
     */
    public String determineAddresses() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return this.host + ":" + this.port;
        }
        List<String> addressStrings = new ArrayList<String>();
        for (Address parsedAddress : this.parsedAddresses) {
            addressStrings.add(parsedAddress.host + ":" + parsedAddress.port);
        }
        return StringUtils.collectionToCommaDelimitedString(addressStrings);
    }

    public void setAddresses(String addresses) {
        this.addresses = addresses;
        this.parsedAddresses = parseAddresses(addresses);
    }

    private List<Address> parseAddresses(String addresses) {
        List<Address> parsedAddresses = new ArrayList<Address>();
        for (String address : StringUtils.commaDelimitedListToStringArray(addresses)) {
            parsedAddresses.add(new Address(address));
        }
        return parsedAddresses;
    }

    /**
     * If addresses have been set and the first address has a username it is returned.
     * Otherwise returns the result of calling {@code getUsername()}.
     *
     * @return the username
     * @see #setAddresses(String)
     * @see #getUsername()
     */
    public String determineUsername() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return this.username;
        }
        Address address = this.parsedAddresses.get(0);
        return address.username == null ? this.username : address.username;
    }

    /**
     * If addresses have been set and the first address has a password it is returned.
     * Otherwise returns the result of calling {@code getPassword()}.
     *
     * @return the password or {@code null}
     * @see #setAddresses(String)
     * @see #getPassword()
     */
    public String determinePassword() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return getPassword();
        }
        Address address = this.parsedAddresses.get(0);
        return address.password == null ? getPassword() : address.password;
    }


    /**
     * If addresses have been set and the first address has a virtual host it is returned.
     * Otherwise returns the result of calling {@code getVirtualHost()}.
     *
     * @return the virtual host or {@code null}
     * @see #setAddresses(String)
     * @see #getVirtualHost()
     */
    public String determineVirtualHost() {
        if (CollectionUtils.isEmpty(this.parsedAddresses)) {
            return getVirtualHost();
        }
        Address address = this.parsedAddresses.get(0);
        return address.virtualHost == null ? getVirtualHost() : address.virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = ("".equals(virtualHost) ? "/" : virtualHost);
    }

    @Data
    public static class Ssl {

        /**
         * Enable SSL support.
         */
        private boolean enabled;

        /**
         * Path to the key store that holds the SSL certificate.
         */
        private String keyStore;

        /**
         * Password used to access the key store.
         */
        private String keyStorePassword;

        /**
         * Trust store that holds SSL certificates.
         */
        private String trustStore;

        /**
         * Password used to access the trust store.
         */
        private String trustStorePassword;

        /**
         * SSL algorithm to use (e.g. TLSv1.1). Default is set automatically by the rabbit
         * client library.
         */
        private String algorithm;

    }

    @Data
    public static class Cache {

        private final Cache.Channel channel = new Cache.Channel();

        private final Cache.Connection connection = new Cache.Connection();

        @Data
        public static class Channel {

            /**
             * Number of channels to retain in the cache. When "check-timeout" > 0, max
             * channels per connection.
             */
            private Integer size;

            /**
             * Number of milliseconds to wait to obtain a channel if the cache size has
             * been reached. If 0, always create a new channel.
             */
            private Long checkoutTimeout;

        }

        @Data
        public static class Connection {

            /**
             * Connection factory cache mode.
             */
            private CachingConnectionFactory.CacheMode mode = CachingConnectionFactory.CacheMode.CHANNEL;

            /**
             * Number of connections to cache. Only applies when mode is CONNECTION.
             */
            private Integer size;

        }

    }

    private static final class Address {

        private static final String PREFIX_AMQP = "amqp://";

        private static final int DEFAULT_PORT = 5672;

        private String host;

        private int port;

        private String username;

        private String password;

        private String virtualHost;

        private Address(String input) {
            input = input.trim();
            input = trimPrefix(input);
            input = parseUsernameAndPassword(input);
            input = parseVirtualHost(input);
            parseHostAndPort(input);
        }

        private String trimPrefix(String input) {
            if (input.startsWith(PREFIX_AMQP)) {
                input = input.substring(PREFIX_AMQP.length());
            }
            return input;
        }

        private String parseUsernameAndPassword(String input) {
            if (input.contains("@")) {
                String[] split = StringUtils.split(input, "@");
                String creds = split[0];
                input = split[1];
                split = StringUtils.split(creds, ":");
                this.username = split[0];
                if (split.length > 0) {
                    this.password = split[1];
                }
            }
            return input;
        }

        private String parseVirtualHost(String input) {
            int hostIndex = input.indexOf("/");
            if (hostIndex >= 0) {
                this.virtualHost = input.substring(hostIndex + 1);
                if (this.virtualHost.isEmpty()) {
                    this.virtualHost = "/";
                }
                input = input.substring(0, hostIndex);
            }
            return input;
        }

        private void parseHostAndPort(String input) {
            int portIndex = input.indexOf(':');
            if (portIndex == -1) {
                this.host = input;
                this.port = DEFAULT_PORT;
            } else {
                this.host = input.substring(0, portIndex);
                this.port = Integer.valueOf(input.substring(portIndex + 1));
            }
        }

    }

    @Data
    public static class AmqpContainer {

        /**
         * Start the container automatically on startup.
         */
        private boolean autoStartup = true;

        /**
         * Acknowledge mode of container.
         */
        private AcknowledgeMode acknowledgeMode;

        /**
         * Minimum number of consumers.
         */
        private Integer concurrency;

        /**
         * Maximum number of consumers.
         */
        private Integer maxConcurrency;

        /**
         * Number of messages to be handled in a single request. It should be greater than
         * or equal to the transaction size (if used).
         */
        private Integer prefetch;

        /**
         * Number of messages to be processed in a transaction. For best results it should
         * be less than or equal to the prefetch count.
         */
        private Integer transactionSize;

        /**
         * Whether rejected deliveries are requeued by default; default true.
         */
        private Boolean defaultRequeueRejected;

        /**
         * How often idle container events should be published in milliseconds.
         */
        private Long idleEventInterval;

        /**
         * Optional properties for a retry interceptor.
         */
        @NestedConfigurationProperty
        private final ListenerRetry retry = new ListenerRetry();

    }

    @Data
    public static class Template {

        @NestedConfigurationProperty
        private final Retry retry = new Retry();

        /**
         * Enable mandatory messages. If a mandatory message cannot be routed to a queue
         * by the server, it will return an unroutable message with a Return method.
         */
        private Boolean mandatory;

        /**
         * Timeout for receive() operations.
         */
        private Long receiveTimeout;

        /**
         * Timeout for sendAndReceive() operations.
         */
        private Long replyTimeout;

    }

    @Data
    public static class Retry {

        /**
         * Whether or not publishing retries are enabled.
         */
        private boolean enabled;

        /**
         * Maximum number of attempts to publish or deliver a message.
         */
        private int maxAttempts = 3;

        /**
         * Interval between the first and second attempt to publish or deliver a message.
         */
        private long initialInterval = 1000L;

        /**
         * A multiplier to apply to the previous retry interval.
         */
        private double multiplier = 1.0;

        /**
         * Maximum interval between attempts.
         */
        private long maxInterval = 10000L;

    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static class ListenerRetry extends Retry {

        /**
         * Whether or not retries are stateless or stateful.
         */
        private boolean stateless = true;

    }

    public static class Listener {

        @NestedConfigurationProperty
        private final AmqpContainer simple = new AmqpContainer();

        public AmqpContainer getSimple() {
            return this.simple;
        }

    }

}
