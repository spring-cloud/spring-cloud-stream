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

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.DirectHandler;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.ReflectionUtils;

/**
 * {@link FactoryBean} for instantiating the interfaces specified via
 * {@link EnableBinding}
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 *
 * @see EnableBinding
 */
public class BindableProxyFactory implements MethodInterceptor, FactoryBean<Object>, Bindable, BeanFactoryAware,
		InitializingBean {

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	private static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	private static final String POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".pollableBridge.interval";

	private static final String CHANNEL_NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".channelNamespace";

	@Value("${" + CHANNEL_NAMESPACE_PROPERTY_NAME + ":}")
	private String channelNamespace;

	@Value("${" + POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME + ":1000}")
	private int pollableBridgeDefaultFrequency;

	@Autowired
	private BindableChannelFactory channelFactory;

	@Autowired(required = false)
	private SharedChannelRegistry sharedChannelRegistry;

	private ConfigurableListableBeanFactory beanFactory;

	private Class<?> type;

	private Object proxy = null;

	private Map<String, ChannelHolder> inputHolders = new HashMap<>();

	private Map<String, ChannelHolder> outputHolders = new HashMap<>();

	public BindableProxyFactory(Class<?> type) {
		this.type = type;
	}

	@Override
	public synchronized Object invoke(MethodInvocation invocation) throws Throwable {
		Method method = invocation.getMethod();
		if (MessageChannel.class.isAssignableFrom(method.getReturnType())) {
			Input input = AnnotationUtils.findAnnotation(method, Input.class);
			if (input != null) {
				String name = BindingBeanDefinitionRegistryUtils.getChannelName(input, method);
				return this.inputHolders.get(name).getMessageChannel();
			}
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = BindingBeanDefinitionRegistryUtils.getChannelName(output, method);
				return this.outputHolders.get(name).getMessageChannel();
			}
		}
		//ignore
		return null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		ReflectionUtils.doWithMethods(type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(input, method);
					Class<? extends MessageChannel> channelType = (Class<? extends MessageChannel>) method.getReturnType();
					MessageChannel sharedChannel = locateSharedChannel(name);
					if (sharedChannel == null) {
						inputHolders.put(name, new ChannelHolder(createBindableChannel(name, channelType), true));
					}
					else {
						inputHolders.put(name, new ChannelHolder(sharedChannel, false));
						if (!channelType.isAssignableFrom(sharedChannel.getClass())) {
							bridgeSharedChannel(channelType, sharedChannel);
						}
					}
				}
			}
		});
		ReflectionUtils.doWithMethods(type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(output, method);
					Class<? extends MessageChannel> channelType = (Class<? extends MessageChannel>) method.getReturnType();
					MessageChannel sharedChannel = locateSharedChannel(name);
					if (sharedChannel == null) {
						outputHolders.put(name, new ChannelHolder(createBindableChannel(name, channelType), true));
					}
					else {
						outputHolders.put(name, new ChannelHolder(sharedChannel, false));
						if (!channelType.isAssignableFrom(sharedChannel.getClass())) {
							bridgeSharedChannel(channelType, sharedChannel);
						}
					}
				}
			}

		});
	}

	private MessageChannel createBindableChannel(String name, Class<? extends MessageChannel> channelType) {
		return isPollable(channelType) ? this.channelFactory.createPollableChannel(name) :
				this.channelFactory.createSubscribableChannel(name);
	}

	private MessageChannel locateSharedChannel(String name) {
		return this.sharedChannelRegistry != null ?
				this.sharedChannelRegistry.get(getNamespacePrefixedChannelName(name)) : null;
	}

	private String getNamespacePrefixedChannelName(String name) {
		return this.channelNamespace + "." + name;
	}

	private void bridgeSharedChannel(Class<? extends MessageChannel> channelType, MessageChannel sharedChannel) {
		// handle the special case where the shared channel is of a different nature
		// (i.e. pollable vs subscribable) than the target channel
		if (isPollable(sharedChannel.getClass())) {
			bridgePollableToSubscribableChannel(sharedChannel, new DirectChannel());
		}
		else {
			bridgeSubscribableToPollableChannel((SubscribableChannel) sharedChannel, new QueueChannel());
		}
	}

	private boolean isPollable(Class<?> channelType) {
		return PollableChannel.class.equals(channelType);
	}

	private void bridgeSubscribableToPollableChannel(SubscribableChannel sharedChannel, MessageChannel inputChannel) {
		sharedChannel.subscribe(new DirectHandler(inputChannel));
	}

	private void bridgePollableToSubscribableChannel(MessageChannel pollableChannel,
			MessageChannel subscribableChannel) {
		ConsumerEndpointFactoryBean consumerEndpointFactoryBean = new ConsumerEndpointFactoryBean();
		consumerEndpointFactoryBean.setInputChannel(pollableChannel);
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(this.pollableBridgeDefaultFrequency));
		consumerEndpointFactoryBean.setPollerMetadata(pollerMetadata);
		consumerEndpointFactoryBean
				.setHandler(new DirectHandler(
						subscribableChannel));
		consumerEndpointFactoryBean.setBeanFactory(this.beanFactory);
		try {
			consumerEndpointFactoryBean.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		consumerEndpointFactoryBean.start();
	}

	@Override
	public synchronized Object getObject() throws Exception {
		if (this.proxy == null) {
			ProxyFactory factory = new ProxyFactory(this.type, this);
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
	public void bindInputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Binding inputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : this.inputHolders.entrySet()) {
			String inputChannelName = channelHolderEntry.getKey();
			ChannelHolder channelHolder = channelHolderEntry.getValue();
			if (channelHolder.isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type, inputChannelName));
				}
				channelBindingService.bindConsumer(channelHolder.getMessageChannel(), inputChannelName);
			}
		}
	}

	@Override
	public void bindOutputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Binding outputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : this.outputHolders.entrySet()) {
			ChannelHolder channelHolder = channelHolderEntry.getValue();
			String outputChannelName = channelHolderEntry.getKey();
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type, outputChannelName));
				}
				channelBindingService.bindProducer(channelHolder.getMessageChannel(), outputChannelName);
			}
		}
	}

	@Override
	public void unbindInputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding inputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : this.inputHolders.entrySet()) {
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Unbinding %s:%s:%s", this.channelNamespace, this.type, channelHolderEntry.getKey()));
				}
				channelBindingService.unbindConsumers(channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public void unbindOutputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding outputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, ChannelHolder> channelHolderEntry : this.outputHolders.entrySet()) {
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type, channelHolderEntry.getKey()));
				}
				channelBindingService.unbindProducers(channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public Set<String> getInputs() {
		return this.inputHolders.keySet();
	}

	@Override
	public Set<String> getOutputs() {
		return this.outputHolders.keySet();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	/**
	 * Holds information about the channels exposed by the interface proxy, as well as
	 * their status.
	 */
	class ChannelHolder {

		private MessageChannel messageChannel;

		private boolean bindable;

		public ChannelHolder(MessageChannel messageChannel, boolean bindable) {
			this.messageChannel = messageChannel;
			this.bindable = bindable;
		}

		public MessageChannel getMessageChannel() {
			return this.messageChannel;
		}

		public boolean isBindable() {
			return this.bindable;
		}
	}
}
