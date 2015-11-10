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

import java.util.HashSet;
import java.util.Set;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderUtils;
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
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * Handles the operations related to channel binding including binding of input/output channels by delegating
 * to an underlying {@link Binder}, setting up data type conversion for binding channel.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ChannelBindingService implements InitializingBean {

	private final Binder<MessageChannel> binder;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private CompositeMessageConverterFactory messageConverterFactory;

	public ChannelBindingService(ChannelBindingServiceProperties channelBindingServiceProperties, Binder<MessageChannel> binder) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.binder = binder;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
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

	public void bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		if (BinderUtils.isChannelPubSub(channelBindingTarget)) {
			BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindings()
					.get(inputChannelName);
			String group = bindingProperties == null ? null : bindingProperties.getGroup();
			this.binder.bindPubSubConsumer(removePrefix(channelBindingTarget),
					inputChannel, group,
					this.channelBindingServiceProperties.getConsumerProperties(inputChannelName));
		}
		else {
			this.binder.bindConsumer(channelBindingTarget, inputChannel,
					this.channelBindingServiceProperties.getConsumerProperties(inputChannelName));
		}
	}

	public void bindProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(outputChannelName);
		if (BinderUtils.isChannelPubSub(channelBindingTarget)) {
			this.binder.bindPubSubProducer(removePrefix(channelBindingTarget),
					outputChannel, this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
		}
		else {
			this.binder.bindProducer(channelBindingTarget, outputChannel,
					this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
		}
	}

	private String removePrefix(String bindingTarget) {
		Assert.isTrue(StringUtils.hasText(bindingTarget), "Binding target should not be empty/null.");
		return bindingTarget.substring(bindingTarget.indexOf(":") + 1);
	}

	public void unbindConsumers(String inputChannelName) {
		this.binder.unbindConsumers(inputChannelName);
	}

	public void unbindProducers(String outputChannelName) {
		this.binder.unbindProducers(outputChannelName);
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	public void configureMessageConverters(Object channel, String channelName) {
		AbstractMessageChannel messageChannel = null;
		try {
			messageChannel = getMessageChannel(channel);
		}
		catch (Exception e) {
			throw new IllegalStateException("Could not get the message channel to configure message converters" + e);
		}
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

	private AbstractMessageChannel getMessageChannel(Object channel) throws Exception {
		if (AopUtils.isJdkDynamicProxy(channel)) {
			return (AbstractMessageChannel) (((Advised) channel).getTargetSource().getTarget());
		}
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		return (AbstractMessageChannel) channel;
	}
}
