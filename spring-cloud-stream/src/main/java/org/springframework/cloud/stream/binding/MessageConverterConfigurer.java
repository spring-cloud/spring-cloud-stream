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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * A {@link MessageChannelConfigurer} that sets data types and message converters based on {@link
 * BindingProperties#contentType}
 * {@link BindingProperties}. This also adds a {@link org.springframework.messaging.support.ChannelInterceptor} to
 * the message channel to set the `ContentType` header for the message (if not already set) based on the `ContentType`
 * binding
 * property of the channel.
 * @author Ilayaperumal Gopinathan
 */
public class MessageConverterConfigurer implements MessageChannelConfigurer, BeanFactoryAware, InitializingBean {

	private ConfigurableListableBeanFactory beanFactory;

	private CompositeMessageConverterFactory messageConverterFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private final Collection<AbstractFromMessageConverter> customMessageConverters;

	private final MessageBuilderFactory messageBuilderFactory;

	public MessageConverterConfigurer(ChannelBindingServiceProperties channelBindingServiceProperties,
									  Collection<AbstractFromMessageConverter> customMessageConverters,
									  MessageBuilderFactory messageBuilderFactory) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.customMessageConverters = customMessageConverters;
		this.messageBuilderFactory = messageBuilderFactory;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.beanFactory, "Bean factory cannot be empty");
		Set<AbstractFromMessageConverter> messageConverters = new HashSet<>();
		if (!CollectionUtils.isEmpty(customMessageConverters)) {
			messageConverters.addAll(Collections.unmodifiableCollection(customMessageConverters));
		}
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

	/**
	 * Setup data-type and message converters for the given message channel.
	 * @param channel     message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	@Override
	public void configureMessageChannel(MessageChannel channel, String channelName) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindingProperties(channelName);
		final String contentType = bindingProperties.getContentType();
		if (bindingProperties != null && StringUtils.hasText(contentType)) {
			MimeType mimeType = MessageConverterUtils.getMimeType(contentType);
			MessageConverter messageConverter = this.messageConverterFactory.newInstance(mimeType);
			Class<?>[] supportedDataTypes = this.messageConverterFactory.supportedDataTypes(mimeType);
			messageChannel.setDatatypes(supportedDataTypes);
			messageChannel.setMessageConverter(messageConverter);
			messageChannel.addInterceptor(new ChannelInterceptorAdapter() {

				@Override
				public Message<?> preSend(Message<?> message, MessageChannel messageChannel) {
					Object contentTypeFromMessage = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
					if (contentTypeFromMessage == null) {
						return messageBuilderFactory
									   .fromMessage(message)
									   .setHeader(MessageHeaders.CONTENT_TYPE, contentType)
									   .build();
					}
					return message;
				}
			});
		}
	}
}
