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
package org.springframework.cloud.stream.config;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.cloud.stream.adapter.ChannelBindingAdapter;
import org.springframework.cloud.stream.adapter.DefaultChannelLocator;
import org.springframework.cloud.stream.adapter.InputChannelBinding;
import org.springframework.cloud.stream.adapter.OutputChannelBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binder.BinderAwareRouterBeanPostProcessor;
import org.springframework.cloud.stream.endpoint.ChannelsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

/**
 * Configuration class that provides necessary beans for {@link MessageChannel} binding.
 *
 * @author Dave Syer
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@Configuration
public class ChannelBindingAdapterConfiguration {

	@Autowired
	private ChannelBindingProperties module;

	@Autowired
	private ConfigurableListableBeanFactory beanFactory;

	@Autowired
	private Binder<MessageChannel> binder;

	@Bean
	public ChannelBindingAdapter bindingAdapter() {
		ChannelBindingAdapter adapter = new ChannelBindingAdapter(this.module, this.binder);
		adapter.setChannelLocator(new DefaultChannelLocator(module));
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
		adapter.setChannelResolver(binderAwareChannelResolver());
		return adapter;
	}

	@Bean
	public ChannelsEndpoint channelsEndpoint(ChannelBindingAdapter adapter) {
		return new ChannelsEndpoint(adapter);
	}

	Collection<OutputChannelBinding> getOutputChannels() {
		Set<OutputChannelBinding> channels = new LinkedHashSet<>();
		String[] names = this.beanFactory.getBeanNamesForType(MessageChannel.class);
		for (String name : names) {
			BeanDefinition beanDefinition = this.beanFactory.getBeanDefinition(name);
			// for now, just assume that the beans are at least AbstractBeanDefinition
			if (beanDefinition instanceof AbstractBeanDefinition
					&& ((AbstractBeanDefinition) beanDefinition).getQualifier(Output.class.getName()) != null) {
				channels.add(new OutputChannelBinding(name));
			}
		}
		return channels;
	}

	Collection<InputChannelBinding> getInputChannels() {
		Set<InputChannelBinding> channels = new LinkedHashSet<>();
		String[] names = this.beanFactory.getBeanNamesForType(MessageChannel.class);
		for (String name : names) {
			BeanDefinition beanDefinition = this.beanFactory.getBeanDefinition(name);
			// for now, just assume that the beans are at least AbstractBeanDefinition
			if (beanDefinition instanceof AbstractBeanDefinition
					&& ((AbstractBeanDefinition) beanDefinition).getQualifier(Input.class.getName()) != null) {
				channels.add(new InputChannelBinding(name));
			}
		}
		return channels;
	}

	@Bean
	public BinderAwareChannelResolver binderAwareChannelResolver() {
		return new BinderAwareChannelResolver(BeanFactoryUtils.beanOfType(beanFactory, Binder.class), new Properties());
	}

	@Bean
	public BinderAwareRouterBeanPostProcessor binderAwareRouterBeanPostProcessor(BinderAwareChannelResolver resolver) {
		return new BinderAwareRouterBeanPostProcessor(resolver);
	}
}
