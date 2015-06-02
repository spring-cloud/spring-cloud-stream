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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

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

import reactor.util.StringUtils;

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

	@Bean
	public ChannelsEndpoint channelsEndpoint(MessageBusAdapter adapter) {
		return new ChannelsEndpoint(module, adapter);
	}

	protected Collection<OutputChannelSpec> getOutputChannels() {
		Set<OutputChannelSpec> channels = new LinkedHashSet<OutputChannelSpec>();
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory,
				MessageChannel.class);
		for (String name : names) {
			String channelName = extractChannelName("output", name,
					module.getOutputChannelName());
			if (channelName != null) {
				OutputChannelSpec channel = new OutputChannelSpec(channelName,
						beanFactory.getBean(name, MessageChannel.class));
				String tapChannelName = !isDefaultOuputChannel(channelName) ? module
						.getTapChannelName(getPlainChannelName(channel.getName()))
						: module.getTapChannelName();
				channel.setTapChannelName(tapChannelName);
				channel.setTapped(false);
				channels.add(channel);
			}
		}
		return channels;
	}

	private boolean isDefaultOuputChannel(String channelName) {
		if (channelName.contains(":")) {
			String[] tokens = channelName.split(":", 2);
			channelName = tokens[1];
		}
		return channelName.equals(module.getOutputChannelName());
	}

	private String extractChannelName(String start, String name,
			String externalChannelName) {
		if (name.equals(start)) {
			return externalChannelName;
		}
		else if (name.startsWith(start + ".") || name.startsWith(start + "_")) {
			String prefix = "";
			String channelName = name.substring(start.length() + 1);
			if (channelName.contains(":")) {
				String[] tokens = channelName.split(":", 2);
				String type = tokens[0];
				if ("queue".equals(type)) {
					// omit the type for a queue
					if (StringUtils.hasText(tokens[1])) {
						prefix = tokens[1] + ".";
					}
				}
				else {
					prefix = channelName + (channelName.endsWith(":") ? "" : ".");
				}
			}
			else {
				prefix = channelName + ".";
			}
			return prefix + getPlainChannelName(externalChannelName);
		}
		return null;
	}

	private String getPlainChannelName(String name) {
		if (name.contains(":")) {
			name = name.substring(name.indexOf(":") + 1);
		}
		return name;
	}

	protected Collection<InputChannelSpec> getInputChannels() {
		Set<InputChannelSpec> channels = new LinkedHashSet<InputChannelSpec>();
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory,
				MessageChannel.class);
		for (String name : names) {
			String channelName = extractChannelName("input", name,
					module.getInputChannelName());
			if (channelName != null) {
				channels.add(new InputChannelSpec(channelName, beanFactory.getBean(name,
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
