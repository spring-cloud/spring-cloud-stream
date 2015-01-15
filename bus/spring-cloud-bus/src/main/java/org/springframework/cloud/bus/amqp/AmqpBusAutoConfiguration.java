package org.springframework.cloud.bus.amqp;

import javax.annotation.PostConstruct;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.messaging.MessageChannel;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnClass(AmqpTemplate.class)
@ConditionalOnExpression("${bus.enabled:true} && ${bus.amqp.enabled:true}")
public class AmqpBusAutoConfiguration {

	public static final String SPRING_CLOUD_BUS = "spring.cloud.bus";

	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	private RabbitTemplate amqpTemplate;

	@Autowired(required = false)
	private ObjectMapper objectMapper;

	@PostConstruct
	public void init() {
		Jackson2JsonMessageConverter converter = messageConverter();
		amqpTemplate.setMessageConverter(converter);
	}

	@Bean
	protected FanoutExchange cloudBusExchange() {
		// TODO: change to TopicExchange?
		FanoutExchange exchange = new FanoutExchange(SPRING_CLOUD_BUS);
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
				.handle(Amqp.outboundAdapter(this.amqpTemplate).exchangeName(
						SPRING_CLOUD_BUS)).get();
	}

	@Bean
	public IntegrationFlow cloudBusAmqpInboundFlow(
			@Qualifier("cloudBusInboundChannel") MessageChannel cloudBusInboundChannel) {
		return IntegrationFlows
				.from(Amqp.inboundAdapter(connectionFactory, localCloudBusQueue())
						.messageConverter(messageConverter()))
				.channel(cloudBusInboundChannel).get();
	}

	private Jackson2JsonMessageConverter messageConverter() {
		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
		if (objectMapper != null) {
			converter.setJsonObjectMapper(objectMapper);
		}
		return converter;
	}

}
