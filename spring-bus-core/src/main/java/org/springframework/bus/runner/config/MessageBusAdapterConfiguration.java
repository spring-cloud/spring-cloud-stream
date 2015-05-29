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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.bus.runner.adapter.InputChannelSpec;
import org.springframework.bus.runner.adapter.MessageBusAdapter;
import org.springframework.bus.runner.adapter.OutputChannelSpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.messaging.MessageChannel;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.MessageBusAwareRouterBeanPostProcessor;

/**
 * @author Dave Syer
 *
 */
@Configuration
@ImportResource("classpath*:/META-INF/spring-xd/bus/codec.xml")
@EnableConfigurationProperties(MessageBusProperties.class)
public class MessageBusAdapterConfiguration {

	@Autowired
	private MessageBusProperties module;

	@Autowired
	private ListableBeanFactory beanFactory;

	@Bean
	public MessageBusAdapter messageBusAdapter(MessageBusProperties module,
			MessageBus messageBus) {
		MessageBusAdapter adapter = new MessageBusAdapter(module, messageBus);
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
		return adapter;
	}

	protected Map<String, OutputChannelSpec> getOutputChannels() {
		Map<String, OutputChannelSpec> channels = new LinkedHashMap<String, OutputChannelSpec>();
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory,
				MessageChannel.class);
		for (String name : names) {
			if (name.equals("output")) {
				String channelName = module.getOutputChannelName();
				channels.put(
						channelName,
						new OutputChannelSpec(channelName, beanFactory.getBean(name,
								MessageChannel.class)));
			}
			else if (name.startsWith("output.")) {
				String channelName = name.substring("output.".length());
				if (channelName.contains(":")) {
					String[] tokens = channelName.split(":", 2);
					String type = tokens[0];
					if ("queue".equals(type)) {
						// omit the type for a queue
						channelName = tokens[1] + "."
								+ getPlainChannelName(module.getOutputChannelName());
					}
					else {
						channelName = channelName + "."
								+ getPlainChannelName(module.getOutputChannelName());
					}
				}
				channels.put(
						channelName,
						new OutputChannelSpec(channelName, beanFactory.getBean(name,
								MessageChannel.class)));
			}
		}
		return channels;
	}

	private String getPlainChannelName(String name) {
		if (name.contains(":")) {
			name = name.substring(name.indexOf(":")+1);
		}
		return name;
	}

	protected Map<String, InputChannelSpec> getInputChannels() {
		Map<String, InputChannelSpec> channels = new LinkedHashMap<String, InputChannelSpec>();
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory,
				MessageChannel.class);
		for (String name : names) {
			if (name.equals("input")) {
				String channelName = module.getInputChannelName();
				channels.put(
						channelName,
						new InputChannelSpec(channelName, beanFactory.getBean(name,
								MessageChannel.class)));
			}
			else if (name.startsWith("input.")) {
				String channelName = name.substring("input.".length());
				if (channelName.contains(":")) {
					String[] tokens = channelName.split(":", 2);
					String type = tokens[0];
					if ("queue".equals(type)) {
						// omit the type for a queue
						channelName = tokens[1] + "."
								+ getPlainChannelName(module.getInputChannelName());
					}
					else {
						channelName = channelName + "."
								+ getPlainChannelName(module.getInputChannelName());
					}
				}
				channels.put(
						channelName,
						new InputChannelSpec(channelName, beanFactory.getBean(name,
								MessageChannel.class)));
			}
		}
		return channels;
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
