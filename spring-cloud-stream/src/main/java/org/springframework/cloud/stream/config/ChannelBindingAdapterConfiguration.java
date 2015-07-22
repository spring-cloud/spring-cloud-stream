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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.LazyInitTargetSource;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.cloud.stream.adapter.ChannelBindingAdapter;
import org.springframework.cloud.stream.adapter.InputChannelBinding;
import org.springframework.cloud.stream.adapter.OutputChannelBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderAwareRouterBeanPostProcessor;
import org.springframework.cloud.stream.endpoint.ChannelsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * @author Dave Syer
 * @author David Turanski
 * @author Marius Bogoevici
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
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
		return adapter;
	}

	@Bean
	public ChannelsEndpoint channelsEndpoint(ChannelBindingAdapter adapter) {
		return new ChannelsEndpoint(adapter);
	}

	public void refresh() {
		ChannelBindingAdapter adapter = bindingAdapter();
		adapter.setOutputChannels(getOutputChannels());
		adapter.setInputChannels(getInputChannels());
	}

	protected Collection<OutputChannelBinding> getOutputChannels() {
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

	protected Collection<InputChannelBinding> getInputChannels() {
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

	// Nested class to avoid instantiating all of the above early
	@Configuration
	protected static class BinderAwareRouterConfiguration {

		@Autowired
		private ListableBeanFactory beanFactory;

		@Bean
		public BinderAwareRouterBeanPostProcessor binderAwareRouterBeanPostProcessor() {

			return new BinderAwareRouterBeanPostProcessor(createLazyProxy(
					this.beanFactory, Binder.class), new Properties());
		}

		private <T> T createLazyProxy(ListableBeanFactory beanFactory, Class<T> type) {
			ProxyFactory factory = new ProxyFactory();
			LazyInitTargetSource source = new LazyInitTargetSource();
			source.setTargetClass(type);
			source.setTargetBeanName(getBeanNameFor(beanFactory, Binder.class));
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
			Assert.state(names.length == 1, "No unique Binder (found " + names.length
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
}
