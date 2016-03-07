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

package org.springframework.cloud.stream.binder.redis.integration;

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

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.redis.RedisMessageChannelBinder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.junit.redis.RedisTestSupport;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Marius Bogoevici
 */
public class RedisBinderModuleTests {

	@ClassRule
	public static RedisTestSupport redisTestSupport = new RedisTestSupport();

	private ConfigurableApplicationContext context = null;

	public static final RedisConnectionFactory MOCK_CONNECTION_FACTORY = Mockito.mock(RedisConnectionFactory.class,
			Mockito.RETURNS_MOCKS);

	@After
	public void tearDown() {
		if (context != null) {
			context.close();
			context = null;
		}
	}

	@Test
	public void testParentConnectionFactoryInheritedByDefault() {
		context = SpringApplication.run(SimpleProcessor.class, "--server.port=0");
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RedisMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		RedisConnectionFactory binderConnectionFactory =
				(RedisConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory, instanceOf(RedisConnectionFactory.class));
		RedisConnectionFactory connectionFactory = context.getBean(RedisConnectionFactory.class);
		assertThat(binderConnectionFactory, is(connectionFactory));
		CompositeHealthIndicator bindersHealthIndicator =
				context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		assertNotNull(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String,HealthIndicator> healthIndicators =
				(Map<String, HealthIndicator>) directFieldAccessor.getPropertyValue("indicators");
		assertThat(healthIndicators, hasKey("redis"));
		assertThat(healthIndicators.get("redis").health().getStatus(), equalTo(Status.UP));
	}

	@Test
	public void testParentConnectionFactoryInheritedIfOverridden() {
		context = new SpringApplication(SimpleProcessor.class, ConnectionFactoryConfiguration.class).run();
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RedisMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		RedisConnectionFactory binderConnectionFactory =
				(RedisConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory, is(MOCK_CONNECTION_FACTORY));
		RedisConnectionFactory connectionFactory = context.getBean(RedisConnectionFactory.class);
		assertThat(binderConnectionFactory, is(connectionFactory));
		CompositeHealthIndicator bindersHealthIndicator =
				context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		assertNotNull(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String,HealthIndicator> healthIndicators =
				(Map<String, HealthIndicator>) directFieldAccessor.getPropertyValue("indicators");
		assertThat(healthIndicators, hasKey("redis"));
		assertThat(healthIndicators.get("redis").health().getStatus(), equalTo(Status.UP));
	}

	@Test
	public void testParentConnectionFactoryNotInheritedByCustomizedBinders() {
		List<String> params = new ArrayList<>();
		params.add("--spring.cloud.stream.input.binder=custom");
		params.add("--spring.cloud.stream.output.binder=custom");
		params.add("--spring.cloud.stream.binders.custom.type=redis");
		params.add("--spring.cloud.stream.binders.custom.environment.foo=bar");
		context = SpringApplication.run(SimpleProcessor.class, params.toArray(new String[]{}));
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RedisMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		RedisConnectionFactory binderConnectionFactory =
				(RedisConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		RedisConnectionFactory connectionFactory = context.getBean(RedisConnectionFactory.class);
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
		public RedisConnectionFactory connectionFactory() {
			return MOCK_CONNECTION_FACTORY;
		}
	}
}
