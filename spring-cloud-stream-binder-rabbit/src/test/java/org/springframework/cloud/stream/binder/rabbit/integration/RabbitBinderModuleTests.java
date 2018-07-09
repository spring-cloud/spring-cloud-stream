/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit.integration;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.*;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 * @author Soby Chacko
 */
public class RabbitBinderModuleTests {

	@ClassRule
	public static RabbitTestSupport rabbitTestSupport = new RabbitTestSupport();

	private ConfigurableApplicationContext context;

	public static final ConnectionFactory MOCK_CONNECTION_FACTORY = mock(ConnectionFactory.class,
			Mockito.RETURNS_MOCKS);

	@After
	public void tearDown() {
		if (context != null) {
			context.close();
			context = null;
		}
		RabbitAdmin admin = new RabbitAdmin(rabbitTestSupport.getResource());
		admin.deleteExchange("input");
		admin.deleteExchange("output");
	}

	@Test
	public void testParentConnectionFactoryInheritedByDefault() {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0", "--spring.cloud.stream.rabbit.binder.connection-name-prefix=foo");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isInstanceOf(CachingConnectionFactory.class);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);

		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		assertThat(bindersHealthIndicator).isNotNull();
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("registry.healthIndicators");
		assertThat(healthIndicators).containsKey(("rabbit"));
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo((Status.UP));

		ConnectionFactory publisherConnectionFactory = binderConnectionFactory.getPublisherConnectionFactory();
		assertThat(TestUtils.getPropertyValue(publisherConnectionFactory, "connection.target")).isNull();
		DirectChannel checkPf = new DirectChannel();
		Binding<MessageChannel> binding = ((RabbitMessageChannelBinder) binder).bindProducer("checkPF", checkPf,
				new ExtendedProducerProperties<>(
						new RabbitProducerProperties()));
		checkPf.send(new GenericMessage<>("foo".getBytes()));
		binding.unbind();
		assertThat(TestUtils.getPropertyValue(publisherConnectionFactory, "connection.target")).isNotNull();

		CachingConnectionFactory cf = this.context.getBean(CachingConnectionFactory.class);
		ConnectionNameStrategy cns = TestUtils.getPropertyValue(cf, "connectionNameStrategy",
				ConnectionNameStrategy.class);
		assertThat(cns.obtainNewConnectionName(cf)).isEqualTo("foo#2");
		new RabbitAdmin(rabbitTestSupport.getResource()).deleteExchange("checkPF");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParentConnectionFactoryInheritedByDefaultAndRabbitSettingsPropagated() {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0",
						"--spring.cloud.stream.bindings.input.group=someGroup",
						"--spring.cloud.stream.rabbit.bindings.input.consumer.transacted=true",
						"--spring.cloud.stream.rabbit.bindings.output.producer.transacted=true");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		BindingService bindingService = context.getBean(BindingService.class);
		DirectFieldAccessor channelBindingServiceAccessor = new DirectFieldAccessor(bindingService);
		Map<String, List<Binding<MessageChannel>>> consumerBindings = (Map<String, List<Binding<MessageChannel>>>) channelBindingServiceAccessor
				.getPropertyValue("consumerBindings");
		Binding<MessageChannel> inputBinding = consumerBindings.get("input").get(0);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(inputBinding,
				"lifecycle.messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(container, "beanName"))
			.isEqualTo("setByCustomizerForQueue:input.someGroup,andGroup:someGroup");
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class)).isTrue();
		Map<String, Binding<MessageChannel>> producerBindings = (Map<String, Binding<MessageChannel>>) TestUtils
				.getPropertyValue(bindingService, "producerBindings");
		Binding<MessageChannel> outputBinding = producerBindings.get("output");
		assertThat(TestUtils.getPropertyValue(outputBinding, "lifecycle.amqpTemplate.transactional",
				Boolean.class)).isTrue();
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isInstanceOf(CachingConnectionFactory.class);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		assertThat(bindersHealthIndicator).isNotNull();
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("registry.healthIndicators");
		assertThat(healthIndicators).containsKey("rabbit");
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo(Status.UP);

		CachingConnectionFactory cf = this.context.getBean(CachingConnectionFactory.class);
		ConnectionNameStrategy cns = TestUtils.getPropertyValue(cf, "connectionNameStrategy",
				ConnectionNameStrategy.class);
		assertThat(cns.obtainNewConnectionName(cf)).startsWith("rabbitConnectionFactory");
	}

	@Test
	public void testParentConnectionFactoryInheritedIfOverridden() {
		context = new SpringApplicationBuilder(SimpleProcessor.class, ConnectionFactoryConfiguration.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isSameAs(MOCK_CONNECTION_FACTORY);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		assertThat(bindersHealthIndicator).isNotNull();
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("registry.healthIndicators");
		assertThat(healthIndicators).containsKey("rabbit");
		// mock connection factory behaves as if down
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	public void testParentConnectionFactoryNotInheritedByCustomizedBindersAndProducerRetryBootProperties() {
		List<String> params = new ArrayList<>();
		params.add("--spring.cloud.stream.input.binder=custom");
		params.add("--spring.cloud.stream.output.binder=custom");
		params.add("--spring.cloud.stream.binders.custom.type=rabbit");
		params.add("--spring.cloud.stream.binders.custom.environment.foo=bar");
		params.add("--server.port=0");
		params.add("--spring.rabbitmq.template.retry.enabled=true");
		params.add("--spring.rabbitmq.template.retry.maxAttempts=2");
		params.add("--spring.rabbitmq.template.retry.initial-interval=1000");
		params.add("--spring.rabbitmq.template.retry.multiplier=1.1");
		params.add("--spring.rabbitmq.template.retry.max-interval=3000");
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run(params.toArray(new String[params.size()]));
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		@SuppressWarnings("unchecked")
		Binder<MessageChannel, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> binder = (Binder<MessageChannel, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>>) binderFactory
				.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isNotSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		assertThat(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("registry.healthIndicators");
		assertThat(healthIndicators).containsKey("custom");
		assertThat(healthIndicators.get("custom").health().getStatus()).isEqualTo(Status.UP);
		String name = UUID.randomUUID().toString();
		Binding<MessageChannel> binding = binder.bindProducer(name, new DirectChannel(),
				new ExtendedProducerProperties<>(new RabbitProducerProperties()));
		RetryTemplate template = TestUtils.getPropertyValue(binding, "lifecycle.amqpTemplate.retryTemplate",
				RetryTemplate.class);
		assertThat(template).isNotNull();
		SimpleRetryPolicy retryPolicy = TestUtils.getPropertyValue(template, "retryPolicy", SimpleRetryPolicy.class);
		ExponentialBackOffPolicy backOff = TestUtils.getPropertyValue(template, "backOffPolicy",
				ExponentialBackOffPolicy.class);
		assertThat(retryPolicy.getMaxAttempts()).isEqualTo(2);
		assertThat(backOff.getInitialInterval()).isEqualTo(1000L);
		assertThat(backOff.getMultiplier()).isEqualTo(1.1);
		assertThat(backOff.getMaxInterval()).isEqualTo(3000L);
		binding.unbind();
		new RabbitAdmin(rabbitTestSupport.getResource()).deleteExchange(name);
		context.close();
	}

	@Test
	public void testCloudProfile() {
		this.context = new SpringApplicationBuilder(SimpleProcessor.class, MockCloudConfiguration.class)
				.web(WebApplicationType.NONE)
				.profiles("cloud")
				.run();
		BinderFactory binderFactory = this.context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		ConnectionFactory connectionFactory = this.context.getBean(ConnectionFactory.class);

		assertThat(binderConnectionFactory).isNotSameAs(connectionFactory);

		assertThat(TestUtils.getPropertyValue(connectionFactory, "addresses")).isNotNull();
		assertThat(TestUtils.getPropertyValue(binderConnectionFactory, "addresses")).isNull();

		Cloud cloud = this.context.getBean(Cloud.class);

		verify(cloud).getSingletonServiceConnector(ConnectionFactory.class, null);
	}

	@Test
	public void testExtendedProperties() {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0", "--spring.cloud.stream.rabbit.default.producer.routing-key-expression=fooRoutingKey",
						"--spring.cloud.stream.rabbit.bindings.output.producer.batch-size=512",
						"--spring.cloud.stream.rabbit.default.consumer.max-concurrency=4",
						"--spring.cloud.stream.rabbit.bindings.input.consumer.exchange-type=fanout");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> rabbitBinder = binderFactory.getBinder(null, MessageChannel.class);

		RabbitProducerProperties rabbitProducerProperties =
				(RabbitProducerProperties)((ExtendedPropertiesBinder) rabbitBinder).getExtendedProducerProperties("output");

		assertThat(rabbitProducerProperties.getRoutingKeyExpression().getExpressionString()).isEqualTo("fooRoutingKey");
		assertThat(rabbitProducerProperties.getBatchSize()).isEqualTo(512);

		RabbitConsumerProperties rabbitConsumerProperties =
				(RabbitConsumerProperties)((ExtendedPropertiesBinder) rabbitBinder).getExtendedConsumerProperties("input");

		assertThat(rabbitConsumerProperties.getExchangeType()).isEqualTo(ExchangeTypes.FANOUT);
		assertThat(rabbitConsumerProperties.getMaxConcurrency()).isEqualTo(4);
	}

	@EnableBinding(Processor.class)
	@SpringBootApplication
	public static class SimpleProcessor {

		@Bean
		public ListenerContainerCustomizer<AbstractMessageListenerContainer> containerCustomizer() {
			return (c, q, g) -> c.setBeanName("setByCustomizerForQueue:" + q +
					(g == null ? "" : ",andGroup:" + g));
		}

	}

	public static class ConnectionFactoryConfiguration {

		@Bean
		public ConnectionFactory connectionFactory() {
			return MOCK_CONNECTION_FACTORY;
		}

	}

	public static class MockCloudConfiguration {

		@Bean
		public Cloud cloud() {
			Cloud cloud = mock(Cloud.class);

			willReturn(new CachingConnectionFactory("localhost"))
							.given(cloud)
							.getSingletonServiceConnector(ConnectionFactory.class, null);

			return cloud;
		}

	}

}
