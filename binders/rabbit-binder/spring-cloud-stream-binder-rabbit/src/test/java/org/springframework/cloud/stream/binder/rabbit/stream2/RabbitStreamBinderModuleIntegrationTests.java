/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.stream2;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.RabbitTestContainer;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties.ContainerType;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 */
public class RabbitStreamBinderModuleIntegrationTests {

	private static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	private ConfigurableApplicationContext context;

	@AfterEach
	void tearDown() {
		if (context != null) {
			context.close();
			context = null;
		}
	}

	@Test
	void testSuperStreamContainer() throws InterruptedException {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		RabbitMessageChannelBinder rabbitBinder = (RabbitMessageChannelBinder) binderFactory.getBinder(null,
				MessageChannel.class);
		RabbitConsumerProperties rProps = new RabbitConsumerProperties();
		rProps.setContainerType(ContainerType.STREAM);
		rProps.setSuperStream(true);
		ExtendedConsumerProperties<RabbitConsumerProperties> props =
				new ExtendedConsumerProperties<RabbitConsumerProperties>(rProps);
		props.setAutoStartup(false);
		props.setInstanceCount(3);
		props.setConcurrency(3);
		Binding<MessageChannel> binding = rabbitBinder.bindConsumer("testSuperStream", "grp", new QueueChannel(), props);
		Object container = TestUtils.getPropertyValue(binding, "lifecycle.messageListenerContainer");
		assertThat(container).isInstanceOf(StreamListenerContainer.class);
		assertThat(container).extracting("concurrency").isEqualTo(3);
		((StreamListenerContainer) container).start();
		assertThat(context.getBean(SimpleProcessor.class).consumerCountLatch.await(10, TimeUnit.SECONDS)).isTrue();
		RabbitAdmin admin = context.getBean(RabbitAdmin.class);
		for (int i = 0; i < 9; i++) {
			Properties qProps = admin.getQueueProperties("testSuperStream-" + i);
			assertThat(qProps).describedAs("Expected queue with index %d to exist", i).isNotNull();
		}
		((StreamListenerContainer) container).stop();
	}

	@SpringBootApplication(proxyBeanMethods = false)
	public static class SimpleProcessor {

		final CountDownLatch consumerCountLatch = new CountDownLatch(3);

		@Bean
		ConnectionFactory cf() {
			return new CachingConnectionFactory(RABBITMQ.getMappedPort(5672));
		}

		@Bean
		RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		Environment rabbitEnvironment() {
			return Environment.builder()
					.addressResolver(add -> new Address("localhost", RABBITMQ.getMappedPort(5552)))
					.build();
		}

		@Bean
		ListenerContainerCustomizer<MessageListenerContainer> containerCustomizer() {
			return (cont, dest, group) -> {
				if (cont instanceof StreamListenerContainer container) {
					container.setConsumerCustomizer((id, builder) -> {
						builder.consumerUpdateListener(context -> {
							this.consumerCountLatch.countDown();
							return OffsetSpecification.first();
						});
					});
				}
			};
		}

	}

}
