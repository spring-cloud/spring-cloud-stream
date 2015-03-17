package org.springframework.cloud.bus.amqp;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.messaging.MessageChannel;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Autoconfiguration for a Spring Cloud Bus on AMQP. Enabled by default if spring-rabbit
 * is on the classpath, and can be switched off with
 * <code>spring.cloud.bus.amqp.enabled</code>. If there is a single
 * {@link ConnectionFactory} in the context it will be used, or if there is a one
 * qualified as <code>@BusConnectionFactory</code> it will be preferred over others,
 * otherwise the <code>@Primary</code> one will be used. If there are multiple unqualified
 * connection factories there will be an autowiring error. Note that Spring Boot (as of
 * 1.2.2) creates a ConnectionFactory that is <i>not</i> <code>@Primary</code>, so if you
 * want to use one connection factory for the bus and another for business messages, you
 * need to create both, and annotate them <code>@BusConnectionFactory</code> and
 * <code>@Primary</code> respectively.
 * 
 * @author Spencer Gibb
 * @author Dave Syer
 */
@Configuration
@ConditionalOnBusEnabled
@ConditionalOnClass({ AmqpTemplate.class, RabbitTemplate.class })
@ConditionalOnProperty(value = "spring.cloud.bus.amqp.enabled", matchIfMissing = true)
@EnableConfigurationProperties(AmqpBusProperties.class)
public class AmqpBusAutoConfiguration {

	@Autowired
	private AmqpBusProperties bus;

	@Autowired(required = false)
	@BusConnectionFactory
	private ConnectionFactory busConnectionFactory;

	@Autowired(required = false)
	private ConnectionFactory primaryConnectionFactory;

	@Autowired
	private ApplicationContext context;

	private RabbitTemplate amqpTemplate;

	@Autowired(required = false)
	private ObjectMapper objectMapper;

	public RabbitTemplate amqpTemplate() {
		if (this.amqpTemplate == null) {
			RabbitTemplate amqpTemplate = new RabbitTemplate(connectionFactory());
			Jackson2JsonMessageConverter converter = messageConverter();
			amqpTemplate.setMessageConverter(converter);
			this.amqpTemplate = amqpTemplate;
			RabbitAdmin amqpAdmin = new RabbitAdmin(connectionFactory());
			cloudBusExchange().setAdminsThatShouldDeclare(amqpAdmin);
			localCloudBusQueueBinding().setAdminsThatShouldDeclare(amqpAdmin);
			localCloudBusQueue().setAdminsThatShouldDeclare(amqpAdmin);
			amqpAdmin.setApplicationContext(context);
			amqpAdmin.afterPropertiesSet();
		}
		return amqpTemplate;
	}

	@Bean
	protected FanoutExchange cloudBusExchange() {
		// TODO: change to TopicExchange?
		FanoutExchange exchange = new FanoutExchange(bus.getExchange());
		return exchange;
	}

	@Bean
	protected Binding localCloudBusQueueBinding() {
		return BindingBuilder.bind(localCloudBusQueue()).to(cloudBusExchange());
	}

	@Bean
	protected Queue localCloudBusQueue() {
		return new AnonymousQueue();
	}

	@Bean
	public IntegrationFlow cloudBusAmqpOutboundFlow(
			@Qualifier("cloudBusOutboundChannel") MessageChannel cloudBusOutboundChannel) {
		return IntegrationFlows
				.from(cloudBusOutboundChannel)
				.handle(Amqp.outboundAdapter(amqpTemplate()).exchangeName(
						bus.getExchange())).get();
	}

	@Bean
	public IntegrationFlow cloudBusAmqpInboundFlow(
			@Qualifier("cloudBusInboundChannel") MessageChannel cloudBusInboundChannel) {
		return IntegrationFlows
				.from(Amqp.inboundAdapter(connectionFactory(), localCloudBusQueue())
						.messageConverter(messageConverter()))
				.channel(cloudBusInboundChannel).get();
	}

	private ConnectionFactory connectionFactory() {
		if (busConnectionFactory != null) {
			return busConnectionFactory;
		}
		return primaryConnectionFactory;
	}

	private Jackson2JsonMessageConverter messageConverter() {
		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
		if (objectMapper != null) {
			converter.setJsonObjectMapper(objectMapper);
		}
		return converter;
	}

}
