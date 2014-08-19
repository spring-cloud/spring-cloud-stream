package org.springframework.platform.bus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.embedded.EmbeddedServletContainerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.integration.core.GenericSelector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.event.inbound.ApplicationEventListeningMessageProducer;
import org.springframework.integration.event.outbound.ApplicationEventPublishingMessageHandler;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.platform.config.client.RefreshEndpoint;
import org.springframework.platform.context.restart.RestartEndpoint;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnClass(AmqpTemplate.class)
public class AmqpBusAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AmqpBusAutoConfiguration.class);
    public static final String X_SPRING_PLATFORM_ORIGIN = "X-Spring-Platform-Origin";

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    AmqpTemplate amqpTemplate;

    @Autowired
    private ConfigurableEnvironment env;

    @Autowired(required = false)
    private RefreshEndpoint refreshEndpoint;

    @Autowired(required = false)
    private RestartEndpoint restartEndpoint;
    private int port;

    //TODO: how to fail gracefully if no rabbit?
    @Bean
    ApplicationListener<EmbeddedServletContainerInitializedEvent> servletInitListener() {
        return new ApplicationListener<EmbeddedServletContainerInitializedEvent>() {
            @Override
            public void onApplicationEvent(EmbeddedServletContainerInitializedEvent event) {
                port = event.getEmbeddedServletContainer().getPort();
            }
        };
    }

    @Bean
    protected FanoutExchange platformBusExchange() {
        //TODO: change to TopicExchange?
        FanoutExchange exchange = new FanoutExchange("spring.platform.bus");
        amqpAdmin.declareExchange(exchange);
        return exchange;
    }

    @Bean
    protected Queue localPlatformBusQueue() {
        Queue queue = amqpAdmin.declareQueue();
        amqpAdmin.declareBinding(BindingBuilder.bind(queue).to(platformBusExchange()));
        return queue;
    }

    @Bean
    public ApplicationEventListeningMessageProducer platformBusProducer() {
        ApplicationEventListeningMessageProducer producer = new ApplicationEventListeningMessageProducer();
        producer.setEventTypes(RemoteApplicationEvent.class);
        producer.setOutputChannel(new DirectChannel());
        return producer;
    }

    @Bean
    public IntegrationFlow platformBusOutboundFlow() {
        return IntegrationFlows.from(platformBusProducer())
                /*.enrichHeaders(new ComponentConfigurer<HeaderEnricherSpec>() {
                    @Override
                    public void configure(HeaderEnricherSpec headers) {
                        headers.header(X_SPRING_PLATFORM_ORIGIN, env.getProperty("spring.application.name"));
                    }
                })*/
                .filter(acceptFromSelf())
                .handle(Amqp.outboundAdapter(this.amqpTemplate)
                        //.mappedRequestHeaders(X_SPRING_PLATFORM_ORIGIN)
                        .exchangeName("spring.platform.bus"))
                .get();
    }

    @Bean
    public GenericSelector acceptFromSelf() {
        return new GenericSelector<RemoteApplicationEvent>() {
            @Override
            public boolean accept(RemoteApplicationEvent source) {
                return AmqpBusAutoConfiguration.this.isFromSelf(source);
            }
        };
    }

    @Bean
    public GenericSelector rejectMessagesFromSelf() {
        /*return new GenericSelector<Message>() {
            @Override
            public boolean accept(Message source) {
                String appName = env.getProperty("spring.application.name");
                Object origin = source.getHeaders().get(X_SPRING_PLATFORM_ORIGIN);
                // don't handle remote messages you sent!
                return !origin.equals(appName);
            }
        };*/
        return new GenericSelector<RemoteApplicationEvent>() {
            @Override
            public boolean accept(RemoteApplicationEvent source) {
                return !AmqpBusAutoConfiguration.this.isFromSelf(source);
            }
        };
    }

    private boolean isFromSelf(RemoteApplicationEvent event) {
        String originService = event.getOriginService();
        String appName = env.getProperty("spring.application.name");
        return originService.equals(appName);
    }

    @Bean
    public IntegrationFlow platformBusInboundFlow(Environment env) {
        ApplicationEventPublishingMessageHandler messageHandler = new ApplicationEventPublishingMessageHandler();
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, localPlatformBusQueue())
                /*.mappedRequestHeaders(X_SPRING_PLATFORM_ORIGIN)*/)
            //TODO: only accept messages to all services or the particular service? should that be at the amqp level?
            .filter(rejectMessagesFromSelf())
            //.channel(MessageChannels.direct().interceptor(new WireTap(wiretapChannel())))
            .handle(messageHandler)
            .get();
    }

    @Bean
    public DirectChannel wiretapChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    @GlobalChannelInterceptor(patterns = "platformBusInboundFlow*")
    public WireTap wireTap() {
        return new WireTap(wiretapChannel());
    }

    @Bean
    public IntegrationFlow loggingFlow() {
        LoggingHandler handler = new LoggingHandler("INFO");
        handler.setShouldLogFullMessage(true);
        return IntegrationFlows.from(wiretapChannel())
                //.filter(rejectMessagesFromSelf())
                .handle(handler)
                .get();
    }
}
