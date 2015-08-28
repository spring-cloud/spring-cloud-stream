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

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.aggregate.SharedChannelRegistry;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * {@link FactoryBean} for instantiating the interfaces specified via
 * {@link org.springframework.cloud.stream.annotation.EnableModule}
 *
 * @author Marius Bogoevici
 * @author David Syer
 *
 * @see org.springframework.cloud.stream.annotation.EnableModule
 */
public class BindableProxyFactory implements MethodInterceptor, FactoryBean<Object>,
		BeanFactoryAware, Bindable, InitializingBean {

	public static final String CHANNEL_NAMESPACE_PROPERTY_NAME = "spring.cloud.stream.internal.channelNamespace";

	private Class<?> type;

	@Value("${" + CHANNEL_NAMESPACE_PROPERTY_NAME + ":}")
	private String channelNamespace;

	private Object proxy = null;

	private Map<String, ChannelHolder> inputs = new HashMap<>();

	private Map<String, ChannelHolder> outputs = new HashMap<>();

	private ConfigurableListableBeanFactory beanFactory;

	@Autowired(required = false)
	private SharedChannelRegistry sharedChannelRegistry;

	public BindableProxyFactory(Class<?> type) {
		this.type = type;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(beanFactory, "Bean factory cannot be empty");
	}

	private void createChannels(Class<?> type) throws Exception {
		ReflectionUtils.doWithMethods(type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException,
					IllegalAccessException {

				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(
							input, method);
					Class<?> inputChannelType = method.getReturnType();
					MessageChannel sharedChannel = locateSharedChannel(name);
					if (sharedChannel == null) {
						MessageChannel inputChannel = createMessageChannel(inputChannelType);
						inputs.put(name, new ChannelHolder(inputChannel, true));
					}
					else {
						if (inputChannelType.isAssignableFrom(sharedChannel.getClass())) {
							inputs.put(name, new ChannelHolder(sharedChannel, false));
						}
						else {
							// handle the special case where the
							final MessageChannel inputChannel = createMessageChannel(inputChannelType);
							if (isPollable(sharedChannel.getClass())) {
								bridgePollableToSubscribableChannel(sharedChannel,
										inputChannel);
							}
							else {
								bridgeSubscribableToPollableChannel(
										(SubscribableChannel) sharedChannel, inputChannel);
							}
							inputs.put(name, new ChannelHolder(inputChannel, false));
						}
					}
				}

				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(
							output, method);
					Class<?> messageChannelType = method.getReturnType();
					MessageChannel sharedChannel = locateSharedChannel(name);
					if (sharedChannel == null) {
						MessageChannel outputChannel = createMessageChannel(messageChannelType);
						outputs.put(name, new ChannelHolder(outputChannel, true));
					}
					else {
						if (messageChannelType.isAssignableFrom(sharedChannel.getClass())) {
							outputs.put(name, new ChannelHolder(sharedChannel, false));
						}
						else {
							final MessageChannel outputChannel = createMessageChannel(messageChannelType);
							if (isPollable(messageChannelType)) {
								bridgePollableToSubscribableChannel(outputChannel,
										sharedChannel);
							}
							else {
								bridgeSubscribableToPollableChannel(
										(SubscribableChannel) outputChannel,
										sharedChannel);
							}
							outputs.put(name, new ChannelHolder(outputChannel, false));
						}
					}
				}
			}
		});
	}

	private MessageChannel locateSharedChannel(String name) {
		return sharedChannelRegistry != null ? sharedChannelRegistry
				.getSharedChannel(getNamespacePrefixedChannelName(name)) : null;
	}

	private String getNamespacePrefixedChannelName(String name) {
		return channelNamespace + "." + name;
	}

	private void bridgeSubscribableToPollableChannel(SubscribableChannel sharedChannel,
			MessageChannel inputChannel) {
		sharedChannel.subscribe(new MessageChannelBinderSupport.DirectHandler(
				inputChannel));
	}

	private void bridgePollableToSubscribableChannel(MessageChannel pollableChannel,
			MessageChannel subscribableChannel) {
		ConsumerEndpointFactoryBean consumerEndpointFactoryBean = new ConsumerEndpointFactoryBean();
		consumerEndpointFactoryBean.setInputChannel(pollableChannel);
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(1000));
		consumerEndpointFactoryBean.setPollerMetadata(pollerMetadata);
		consumerEndpointFactoryBean
				.setHandler(new MessageChannelBinderSupport.DirectHandler(
						subscribableChannel));
		consumerEndpointFactoryBean.setBeanFactory(beanFactory);
		try {
			consumerEndpointFactoryBean.afterPropertiesSet();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		consumerEndpointFactoryBean.start();
	}

	private MessageChannel createMessageChannel(Class<?> messageChannelType) {
		return isPollable(messageChannelType) ? new QueueChannel() : new DirectChannel();
	}

	private boolean isPollable(Class<?> channelType) {
		return PollableChannel.class.equals(channelType);
	}

	@Override
	public synchronized Object invoke(MethodInvocation invocation) throws Throwable {
		Method method = invocation.getMethod();
		if (MessageChannel.class.isAssignableFrom(method.getReturnType())) {
			Input input = AnnotationUtils.findAnnotation(method, Input.class);
			if (input != null) {
				String name = BindingBeanDefinitionRegistryUtils.getChannelName(input,
						method);
				return this.inputs.get(name).getMessageChannel();
			}
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = BindingBeanDefinitionRegistryUtils.getChannelName(output,
						method);
				return this.outputs.get(name).getMessageChannel();
			}
		}
		// ignore
		return null;
	}

	@Override
	public synchronized Object getObject() throws Exception {
		if (this.proxy == null) {
			createChannels(this.type);
			ProxyFactory factory = new ProxyFactory(type, this);
			this.proxy = factory.getProxy();
		}
		return this.proxy;
	}

	@Override
	public Class<?> getObjectType() {
		return this.type;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void bindInputs(ChannelBindingAdapter channelBindingAdapter) {
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : inputs.entrySet()) {
			ChannelHolder channelHolder = channelHolderEntry.getValue();
			if (channelHolder.isBound()) {
				channelBindingAdapter.bindMessageConsumer(
						channelHolder.getMessageChannel(), channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public void bindOutputs(ChannelBindingAdapter channelBindingAdapter) {
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : outputs.entrySet()) {
			if (channelHolderEntry.getValue().isBound()) {
				channelBindingAdapter.bindMessageProducer(channelHolderEntry.getValue()
						.getMessageChannel(), channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public void unbindInputs(ChannelBindingAdapter channelBindingAdapter) {
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : inputs.entrySet()) {
			if (channelHolderEntry.getValue().isBound()) {
				channelBindingAdapter.unbindConsumers(channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public void unbindOutputs(ChannelBindingAdapter channelBindingAdapter) {
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : outputs.entrySet()) {
			if (channelHolderEntry.getValue().isBound()) {
				channelBindingAdapter.unbindProducers(channelHolderEntry.getKey());
			}
		}
	}

	/**
	 * Holds information about the channels exposed by the interface proxy, as well as
	 * their status.
	 *
	 */
	class ChannelHolder {

		private MessageChannel messageChannel;

		private boolean bound;

		public ChannelHolder(MessageChannel messageChannel, boolean bound) {
			this.messageChannel = messageChannel;
			this.bound = bound;
		}

		public MessageChannel getMessageChannel() {
			return messageChannel;
		}

		public boolean isBound() {
			return bound;
		}

	}

}
