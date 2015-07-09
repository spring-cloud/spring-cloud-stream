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
package org.springframework.cloud.streams.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.LazyInitTargetSource;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.cloud.streams.adapter.ChannelBindingAdapter;
import org.springframework.cloud.streams.adapter.ChannelLocator;
import org.springframework.cloud.streams.adapter.Input;
import org.springframework.cloud.streams.adapter.InputChannelBinding;
import org.springframework.cloud.streams.adapter.Output;
import org.springframework.cloud.streams.adapter.OutputChannelBinding;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.FileKryoRegistrar;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.KryoRegistrar;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.PojoCodec;
import org.springframework.cloud.streams.endpoint.ChannelsEndpoint;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.MessageBusAwareRouterBeanPostProcessor;


/**
 * @author Dave Syer
 * @author David Turanski
 */
@Configuration

public class ChannelBindingAdapterConfiguration {

	@Autowired
	private ChannelBindingProperties module;

	@Autowired
	private ListableBeanFactory beanFactory;

	@Autowired(required = false)
	@Input
	private ChannelLocator inputChannelLocator;

	@Autowired(required = false)
	@Output
	private ChannelLocator outputChannelLocator;

	@Autowired
	private MessageBus messageBus;

	@Bean
	public ChannelBindingAdapter messageBusAdapter() {
		ChannelBindingAdapter adapter = new ChannelBindingAdapter(this.module, this.messageBus);
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
		if (this.inputChannelLocator != null) {
			adapter.setInputChannelLocator(this.inputChannelLocator);
		}
		if (this.outputChannelLocator != null) {
			adapter.setOutputChannelLocator(this.outputChannelLocator);
		}
		return adapter;
	}

	@Bean
	public ChannelsEndpoint channelsEndpoint(ChannelBindingAdapter adapter) {
		return new ChannelsEndpoint(adapter);
	}

	public void refresh() {
		ChannelBindingAdapter adapter = messageBusAdapter();
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
	}

	protected Collection<OutputChannelBinding> getOutputChannels() {
		Set<OutputChannelBinding> channels = new LinkedHashSet<OutputChannelBinding>();
		String[] names = this.beanFactory.getBeanNamesForType(MessageChannel.class);
		for (String name : names) {
			if (name.startsWith("output")) {
				channels.add(new OutputChannelBinding(name));
			}
		}
		return channels;
	}

	protected Collection<InputChannelBinding> getInputChannels() {
		Set<InputChannelBinding> channels = new LinkedHashSet<InputChannelBinding>();
		String[] names = this.beanFactory.getBeanNamesForType(MessageChannel.class);
		for (String name : names) {
			if (name.startsWith("input")) {
				channels.add(new InputChannelBinding(name));
			}
		}
		return channels;
	}

	// Nested class to avoid instantiating all of the above early
	@Configuration
	protected static class MessageBusAwareRouterConfiguration {

		@Autowired
		private ListableBeanFactory beanFactory;

		@Bean
		public MessageBusAwareRouterBeanPostProcessor messageBusAwareRouterBeanPostProcessor() {

			return new MessageBusAwareRouterBeanPostProcessor(createLazyProxy(
					this.beanFactory, MessageBus.class), new Properties());
		}

		private <T> T createLazyProxy(ListableBeanFactory beanFactory, Class<T> type) {
			ProxyFactory factory = new ProxyFactory();
			LazyInitTargetSource source = new LazyInitTargetSource();
			source.setTargetClass(type);
			source.setTargetBeanName(getBeanNameFor(beanFactory, MessageBus.class));
			source.setBeanFactory(beanFactory);
			factory.setTargetSource(source);
			factory.addAdvice(new PassthruAdvice());
			factory.setInterfaces(new Class<?>[] {type});
			@SuppressWarnings("unchecked")
			T proxy = (T) factory.getProxy();
			return proxy;
		}

		private String getBeanNameFor(ListableBeanFactory beanFactory, Class<?> type) {
			String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(
					beanFactory, type, false, false);
			Assert.state(names.length == 1, "No unique MessageBus (found " + names.length
					+ ": " + Arrays.asList(names) + ")");
			return names[0];
		}

		private class PassthruAdvice implements MethodInterceptor {

			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				return invocation.proceed();
			}

		}

	}

	@Configuration
	@ConditionalOnMissingBean(value=ChannelBindingProperties.class, search=SearchStrategy.CURRENT)
	protected static class ModulePropertiesConfiguration {
		@Bean(name = "spring.cloud.channels.CONFIGURATION_PROPERTIES")
		public ChannelBindingProperties moduleProperties() {
			return new ChannelBindingProperties();
		}
	}


	protected static class CodecConfiguration {
		@Autowired
		ApplicationContext applicationContext;

		@Bean
		@ConditionalOnMissingBean(name = "codec")
		public MultiTypeCodec codec() {
			Map<String, KryoRegistrar> kryoRegistrarMap = applicationContext.getBeansOfType(KryoRegistrar
					.class);
			return new PojoCodec(new ArrayList<>(kryoRegistrarMap.values()));
		}

		@Bean
		public KryoRegistrar fileRegistrar() {
			return new FileKryoRegistrar();
		}
	}

}
