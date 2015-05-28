/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.bus.runner.config;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.bus.runner.adapter.MessageBusAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.messaging.MessageChannel;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.MessageBusAwareRouterBeanPostProcessor;

/**
 * @author Dave Syer
 *
 */
@Configuration
@Import(PropertyPlaceholderAutoConfiguration.class)
@ImportResource("classpath*:/META-INF/spring-xd/bus/codec.xml")
@EnableConfigurationProperties(MessageBusProperties.class)
public class MessageBusAdapterConfiguration {

	@Autowired(required = false)
	@Qualifier("output")
	private MessageChannel output;

	@Autowired(required = false)
	@Qualifier("input")
	private MessageChannel input;

	@Bean
	public MessageBusAdapter messageBusAdapter(MessageBusProperties module,
			MessageBus messageBus) {
		MessageBusAdapter adapter = new MessageBusAdapter(module, messageBus);
		adapter.setOutputChannel(output);
		adapter.setInputChannel(input);
		return adapter;
	}

	// Nested class to avoid instantiating all of the above early
	@Configuration
	protected static class MessageBusAwareRouterConfiguration {

		@Bean
		public MessageBusAwareRouterBeanPostProcessor messageBusAwareRouterBeanPostProcessor(
				MessageBus messageBus) {
			return new MessageBusAwareRouterBeanPostProcessor(messageBus,
					new Properties());
		}

	}

}
