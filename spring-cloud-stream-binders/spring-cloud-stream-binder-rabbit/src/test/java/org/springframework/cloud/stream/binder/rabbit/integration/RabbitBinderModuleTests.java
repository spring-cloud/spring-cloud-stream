/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.junit.rabbit.RabbitTestSupport;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class RabbitBinderModuleTests {

	@ClassRule
	public static RabbitTestSupport rabbitTestSupport = new RabbitTestSupport();

	private ConfigurableApplicationContext context = null;

	public static final ConnectionFactory MOCK_CONNECTION_FACTORY =
			Mockito.mock(ConnectionFactory.class, Mockito.RETURNS_MOCKS);

	@After
	public void tearDown() {
		if (context != null) {
			context.close();
			context = null;
		}
		RabbitAdmin admin = new RabbitAdmin(rabbitTestSupport.getResource());
		admin.deleteQueue("binder.input.default");
		admin.deleteQueue("binder.output.default");
		admin.deleteExchange("binder.input");
		admin.deleteExchange("binder.output");
	}

	@Test
	public void testParentConnectionFactoryInheritedByDefault() {
		context = SpringApplication.run(SimpleProcessor.class, "--server.port=0");
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RabbitMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory =
				(ConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory, instanceOf(CachingConnectionFactory.class));
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory, is(connectionFactory));
		CompositeHealthIndicator bindersHealthIndicator =
				context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		assertNotNull(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String,HealthIndicator> healthIndicators =
				(Map<String, HealthIndicator>) directFieldAccessor.getPropertyValue("indicators");
		assertThat(healthIndicators, hasKey("rabbit"));
		assertThat(healthIndicators.get("rabbit").health().getStatus(), equalTo(Status.UP));
	}

	@Test
	public void testParentConnectionFactoryInheritedIfOverridden() {
		context = new SpringApplication(SimpleProcessor.class, ConnectionFactoryConfiguration.class).run("--server.port=0");
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RabbitMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory =
				(ConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory, is(MOCK_CONNECTION_FACTORY));
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory, is(connectionFactory));
		CompositeHealthIndicator bindersHealthIndicator =
				context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		assertNotNull(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String,HealthIndicator> healthIndicators =
				(Map<String, HealthIndicator>) directFieldAccessor.getPropertyValue("indicators");
		assertThat(healthIndicators, hasKey("rabbit"));
		// mock connection factory behaves as if down
		assertThat(healthIndicators.get("rabbit").health().getStatus(), equalTo(Status.DOWN));
	}

	@Test
	public void testParentConnectionFactoryNotInheritedByCustomizedBinders() {
		List<String> params = new ArrayList<>();
		params.add("--spring.cloud.stream.input.binder=custom");
		params.add("--spring.cloud.stream.output.binder=custom");
		params.add("--spring.cloud.stream.binders.custom.type=rabbit");
		params.add("--spring.cloud.stream.binders.custom.environment.foo=bar");
		params.add("--server.port=0");
		context = SpringApplication.run(SimpleProcessor.class, params.toArray(new String[params.size()]));
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RabbitMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory =
				(ConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory, not(is(connectionFactory)));
		CompositeHealthIndicator bindersHealthIndicator =
				context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		assertNotNull(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String,HealthIndicator> healthIndicators =
				(Map<String, HealthIndicator>) directFieldAccessor.getPropertyValue("indicators");
		assertThat(healthIndicators, hasKey("custom"));
		assertThat(healthIndicators.get("custom").health().getStatus(), equalTo(Status.UP));
	}

	@EnableBinding(Processor.class)
	@SpringBootApplication
	public static class SimpleProcessor {

	}

	public static class ConnectionFactoryConfiguration {

		@Bean
		public ConnectionFactory connectionFactory() {
			return MOCK_CONNECTION_FACTORY;
		}

	}

}
