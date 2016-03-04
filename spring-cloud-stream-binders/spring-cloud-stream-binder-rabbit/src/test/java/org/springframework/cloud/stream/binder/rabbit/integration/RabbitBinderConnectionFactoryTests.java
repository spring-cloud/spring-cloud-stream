/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.junit.rabbit.RabbitTestSupport;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Ilayaperumal Gopinathan
 */
public class RabbitBinderConnectionFactoryTests {

	@ClassRule
	public static RabbitTestSupport rabbitTestSupport = new RabbitTestSupport();

	private ConfigurableApplicationContext context = null;

	@Test
	public void testChannelCacheSizeInConnectionFactory() {
		List<String> params = new ArrayList<>();
		params.add("--spring.rabbitmq.channelCacheSize=100");
		params.add("--server.port=0");
		context = SpringApplication.run(SimpleProcessor.class, params.toArray(new String[params.size()]));
		BinderFactory<?> binderFactory = context.getBean(BinderFactory.class);
		Binder<?> binder = binderFactory.getBinder(null);
		assertThat(binder, instanceOf(RabbitMessageChannelBinder.class));
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		CachingConnectionFactory binderConnectionFactory =
				(CachingConnectionFactory) binderFieldAccessor.getPropertyValue("connectionFactory");
		assertTrue("Channel Cache Size must be set correctly.", binderConnectionFactory.getChannelCacheSize() == 100);
	}

	@EnableBinding(Processor.class)
	@SpringBootApplication
	public static class SimpleProcessor {

	}
}
