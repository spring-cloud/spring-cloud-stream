/*
 * Copyright 2021-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit.stream;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.ProducerBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.RabbitTestContainer;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties.ContainerType;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.ProducerType;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.outbound.RabbitStreamMessageHandler;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Soby Chacko
 */
class RabbitStreamBinderModuleTests {

	private static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	@Test
	void streamContainer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0")) {
			BinderFactory binderFactory = context.getBean(BinderFactory.class);
			RabbitMessageChannelBinder rabbitBinder = (RabbitMessageChannelBinder) binderFactory.getBinder(null,
					MessageChannel.class);
			RabbitConsumerProperties rProps = new RabbitConsumerProperties();
			rProps.setContainerType(ContainerType.STREAM);
			ExtendedConsumerProperties<RabbitConsumerProperties> props =
					new ExtendedConsumerProperties<RabbitConsumerProperties>(rProps);
			props.setAutoStartup(false);
			Binding<MessageChannel> binding = rabbitBinder.bindConsumer("testStream", "grp", new QueueChannel(), props);
			Object container = TestUtils.getPropertyValue(binding, "lifecycle.messageListenerContainer");
			assertThat(container).isInstanceOf(StreamListenerContainer.class);
			((StreamListenerContainer) container).start();
			verify(context.getBean(ConsumerBuilder.class)).offset(OffsetSpecification.first());
			((StreamListenerContainer) container).stop();
		}
	}

	@Test
	void superStreamContainer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0")) {
			BinderFactory binderFactory = context.getBean(BinderFactory.class);
			RabbitMessageChannelBinder rabbitBinder = (RabbitMessageChannelBinder) binderFactory.getBinder(null,
					MessageChannel.class);
			RabbitConsumerProperties rProps = new RabbitConsumerProperties();
			rProps.setContainerType(ContainerType.STREAM);
			rProps.setSuperStream(true);
			ExtendedConsumerProperties<RabbitConsumerProperties> props =
					new ExtendedConsumerProperties<RabbitConsumerProperties>(rProps);
			props.setAutoStartup(false);
			props.setInstanceCount(1);
			Binding<MessageChannel> binding = rabbitBinder.bindConsumer("testSuperStream", "grp", new QueueChannel(), props);
			Object container = TestUtils.getPropertyValue(binding, "lifecycle.messageListenerContainer");
			assertThat(container).isInstanceOf(StreamListenerContainer.class);
			((StreamListenerContainer) container).start();
			ConsumerBuilder builder = context.getBean(ConsumerBuilder.class);
			verify(builder).singleActiveConsumer();
			verify(builder).superStream("testSuperStream");
			verify(builder).name("testSuperStream.grp");
			((StreamListenerContainer) container).stop();
		}
	}

	@Test
	void streamHandler() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0")) {
			BinderFactory binderFactory = context.getBean(BinderFactory.class);
			RabbitMessageChannelBinder rabbitBinder = (RabbitMessageChannelBinder) binderFactory.getBinder(null,
					MessageChannel.class);
			RabbitProducerProperties rProps = new RabbitProducerProperties();
			rProps.setProducerType(ProducerType.STREAM_SYNC);
			ExtendedProducerProperties<RabbitProducerProperties> props =
					new ExtendedProducerProperties<RabbitProducerProperties>(rProps);
			Binding<MessageChannel> binding = rabbitBinder.bindProducer("testStream", new DirectChannel(), props);
			Object handler = TestUtils.getPropertyValue(binding, "val$producerMessageHandler");
			assertThat(handler).isInstanceOf(RabbitStreamMessageHandler.class);
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class SimpleProcessor {

		@Bean
		ConnectionFactory cf() {
			return new CachingConnectionFactory(RABBITMQ.getMappedPort(5672));
		}

		@Bean
		ProducerMessageHandlerCustomizer<MessageHandler> handlerCustomizer() {
			return (hand, dest) -> {
				RabbitStreamMessageHandler handler = (RabbitStreamMessageHandler) hand;
				handler.setConfirmTimeout(5000);
			};
		}

		@Bean
		ListenerContainerCustomizer<MessageListenerContainer> containerCustomizer() {
			return (cont, dest, group) -> {
				StreamListenerContainer container = (StreamListenerContainer) cont;
				container.setConsumerCustomizer((name, builder) -> {
					builder.offset(OffsetSpecification.first());
				});
			};
		}

		@Bean
		Environment rabbitStreamEnvironment(ConsumerBuilder consumerBuilder, ProducerBuilder producerBuilder) {
			Environment env = mock(Environment.class);
			given(env.consumerBuilder()).willReturn(consumerBuilder);
			given(env.producerBuilder()).willReturn(producerBuilder);
			return env;
		}

		@Bean
		ConsumerBuilder consumerBuilder() {
			ConsumerBuilder mock = mock(ConsumerBuilder.class);
			given(mock.superStream(anyString())).willReturn(mock);
			given(mock.singleActiveConsumer()).willReturn(mock);
			return mock;
		}

		@Bean
		ProducerBuilder producerBuilder() {
			return mock(ProducerBuilder.class);
		}

	}

}
