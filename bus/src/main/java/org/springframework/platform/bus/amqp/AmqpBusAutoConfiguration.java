package org.springframework.platform.bus.amqp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
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
import org.springframework.platform.bus.event.RemoteApplicationEvent;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnClass(AmqpTemplate.class)
@ConditionalOnExpression("${bus.amqp.enabled:true}")
public class AmqpBusAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AmqpBusAutoConfiguration.class);
    public static final String SPRING_PLATFORM_BUS = "spring.platform.bus";

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Autowired
    private ConfigurableEnvironment env;

    //TODO: how to fail gracefully if no rabbit?
    @Bean
    protected FanoutExchange platformBusExchange() {
        //TODO: change to TopicExchange?
        FanoutExchange exchange = new FanoutExchange(SPRING_PLATFORM_BUS);
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
                .filter(acceptFromSelf())
                .handle(Amqp.outboundAdapter(this.amqpTemplate).exchangeName(SPRING_PLATFORM_BUS))
                .get();
    }

    //TODO: is there a way to move these filters to rabbit while not loosing the information once it is published to spring?
    @Bean
    public GenericSelector acceptFromSelf() {
        return new GenericSelector<RemoteApplicationEvent>() {
            @Override
            public boolean accept(RemoteApplicationEvent source) {
                return isFromSelf(source);
            }
        };
    }

    @Bean
    public GenericSelector inboutFilter() {
        return new GenericSelector<RemoteApplicationEvent>() {
            @Override
            public boolean accept(RemoteApplicationEvent event) {
                return !isFromSelf(event) && isForSelf(event);
            }
        };
    }

    private boolean isForSelf(RemoteApplicationEvent event) {
        return (event.getDestinationService() == null || event.getDestinationService().equals(getAppName()));
    }

    private boolean isFromSelf(RemoteApplicationEvent event) {
        String originService = event.getOriginService();
        String appName = getAppName();
        return originService.equals(appName);
    }

    private String getAppName() {
        return env.getProperty("spring.application.name");
    }

    @Bean
    public IntegrationFlow platformBusInboundFlow(Environment env) {
        ApplicationEventPublishingMessageHandler messageHandler = new ApplicationEventPublishingMessageHandler();
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, localPlatformBusQueue()))
            .filter(inboutFilter())
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
                .handle(handler)
                .get();
    }
}
