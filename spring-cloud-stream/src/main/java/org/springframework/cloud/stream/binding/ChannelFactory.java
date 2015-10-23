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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.aggregate.SharedChannelRegistry;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.AbstractFromMessageConverter;
import org.springframework.cloud.stream.converter.ByteArrayToStringMessageConverter;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.JavaToSerializedMessageConverter;
import org.springframework.cloud.stream.converter.JsonToPojoMessageConverter;
import org.springframework.cloud.stream.converter.JsonToTupleMessageConverter;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.converter.PojoToJsonMessageConverter;
import org.springframework.cloud.stream.converter.PojoToStringMessageConverter;
import org.springframework.cloud.stream.converter.SerializedToJavaMessageConverter;
import org.springframework.cloud.stream.converter.StringToByteArrayMessageConverter;
import org.springframework.cloud.stream.converter.TupleToJsonMessageConverter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Class that {@link BindableProxyFactory} uses to create message channels.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
public class ChannelFactory implements BeanFactoryAware, InitializingBean {

	public static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	public static final String CHANNEL_NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".channelNamespace";

	public static final String POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".pollableBridge.interval";

	@Value("${" + CHANNEL_NAMESPACE_PROPERTY_NAME + ":}")
	private String channelNamespace;

	@Value("${" + POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME + ":1000}")
	private int pollableBridgeDefaultFrequency;

	private ConfigurableListableBeanFactory beanFactory;

	@Autowired(required = false)
	private SharedChannelRegistry sharedChannelRegistry;

	private CompositeMessageConverterFactory messageConverterFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private Map<String, ChannelHolder> inputs = new HashMap<>();

	private Map<String, ChannelHolder> outputs = new HashMap<>();

	Map<String, ChannelHolder> getInputs() {
		return this.inputs;
	}

	Map<String, ChannelHolder> getOutputs() {
		return this.outputs;
	}

	public ChannelFactory(ChannelBindingServiceProperties channelBindingServiceProperties) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.beanFactory, "Bean factory cannot be empty");
		Set<AbstractFromMessageConverter> messageConverters = new HashSet<>();
		messageConverters.add(new JsonToTupleMessageConverter());
		messageConverters.add(new TupleToJsonMessageConverter());
		messageConverters.add(new JsonToPojoMessageConverter());
		messageConverters.add(new PojoToJsonMessageConverter());
		messageConverters.add(new ByteArrayToStringMessageConverter());
		messageConverters.add(new StringToByteArrayMessageConverter());
		messageConverters.add(new PojoToStringMessageConverter());
		messageConverters.add(new JavaToSerializedMessageConverter());
		messageConverters.add(new SerializedToJavaMessageConverter());
		this.messageConverterFactory = new CompositeMessageConverterFactory(messageConverters);
	}

	void createChannels(Class<?> type) throws Exception {
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
						MessageChannel inputChannel = createMessageChannel(inputChannelType, name);
						inputs.put(name, new ChannelHolder(inputChannel, true));
						configureMessageConverters(inputChannel, name);
					}
					else {
						if (inputChannelType.isAssignableFrom(sharedChannel.getClass())) {
							inputs.put(name, new ChannelHolder(sharedChannel, false));
							configureMessageConverters(sharedChannel, name);
						}
						else {
							// handle the special case where the shared channel is of a different nature
							// (i.e. pollable vs subscribable) than the target channel
							final MessageChannel inputChannel = createMessageChannel(inputChannelType, name);
							if (isPollable(sharedChannel.getClass())) {
								bridgePollableToSubscribableChannel(sharedChannel,
										inputChannel);
							}
							else {
								bridgeSubscribableToPollableChannel(
										(SubscribableChannel) sharedChannel, inputChannel);
							}
							inputs.put(name, new ChannelHolder(inputChannel, false));
							configureMessageConverters(inputChannel, name);
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
						MessageChannel outputChannel = createMessageChannel(messageChannelType, name);
						outputs.put(name, new ChannelHolder(outputChannel, true));
						configureMessageConverters(outputChannel, name);
					}
					else {
						if (messageChannelType.isAssignableFrom(sharedChannel.getClass())) {
							outputs.put(name, new ChannelHolder(sharedChannel, false));
							configureMessageConverters(sharedChannel, name);
						}
						else {
							// handle the special case where the shared channel is of a different nature
							// (i.e. pollable vs subscribable) than the target channel
							final MessageChannel outputChannel = createMessageChannel(messageChannelType, name);
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
							configureMessageConverters(outputChannel, name);
						}
					}
				}
			}
		});
	}

	private MessageChannel locateSharedChannel(String name) {
		return this.sharedChannelRegistry != null ? this.sharedChannelRegistry.get(getNamespacePrefixedChannelName(name)) : null;
	}

	private String getNamespacePrefixedChannelName(String name) {
		return this.channelNamespace + "." + name;
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
		pollerMetadata.setTrigger(new PeriodicTrigger(this.pollableBridgeDefaultFrequency));
		consumerEndpointFactoryBean.setPollerMetadata(pollerMetadata);
		consumerEndpointFactoryBean
				.setHandler(new MessageChannelBinderSupport.DirectHandler(
						subscribableChannel));
		consumerEndpointFactoryBean.setBeanFactory(this.beanFactory);
		try {
			consumerEndpointFactoryBean.afterPropertiesSet();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		consumerEndpointFactoryBean.start();
	}

	private MessageChannel createMessageChannel(Class<?> messageChannelType, String channelName) {
		MessageChannel messageChannel = isPollable(messageChannelType) ? new QueueChannel() : new DirectChannel();
		return messageChannel;
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	private void configureMessageConverters(MessageChannel channel, String channelName) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindings().get(channelName);
		if (bindingProperties != null) {
			String contentType = bindingProperties.getContentType();
			if (StringUtils.hasText(contentType)) {
				MimeType mimeType = MessageConverterUtils.getMimeType(contentType);
				MessageConverter messageConverter = this.messageConverterFactory.newInstance(mimeType);
				Class<?> dataType = MessageConverterUtils.getJavaTypeForContentType(mimeType,
						Thread.currentThread().getContextClassLoader());
				messageChannel.setDatatypes(dataType);
				messageChannel.setMessageConverter(messageConverter);
			}
		}
	}

	private boolean isPollable(Class<?> channelType) {
		return PollableChannel.class.equals(channelType);
	}
}
