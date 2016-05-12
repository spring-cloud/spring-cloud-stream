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


import java.util.Collections;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * A {@link MessageChannelConfigurer} that sets data types and message converters based on {@link
 * BindingProperties#contentType}. Also adds a {@link org.springframework.messaging.support.ChannelInterceptor} to
 * the message channel to set the `ContentType` header for the message (if not already set) based on the `ContentType`
 * binding property of the channel.
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
public class MessageConverterConfigurer implements MessageChannelConfigurer, BeanFactoryAware, InitializingBean {

	private final MessageBuilderFactory messageBuilderFactory;

	private ConfigurableListableBeanFactory beanFactory;

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	public MessageConverterConfigurer(ChannelBindingServiceProperties channelBindingServiceProperties,
			MessageBuilderFactory messageBuilderFactory,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		Assert.notNull(compositeMessageConverterFactory, "The message converter factory cannot be null");
		this.messageBuilderFactory = messageBuilderFactory;
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.beanFactory, "Bean factory cannot be empty");
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
		if (StringUtils.hasText(contentType)) {
			MimeType mimeType = MessageConverterUtils.getMimeType(contentType);
			SmartMessageConverter messageConverter = this.compositeMessageConverterFactory.getMessageConverterForType(mimeType);
			Class<?>[] supportedDataTypes = this.compositeMessageConverterFactory.supportedDataTypes(mimeType);
			messageChannel.setDatatypes(supportedDataTypes);
			messageChannel.setMessageConverter(new MessageWrappingMessageConverter(messageConverter, mimeType));
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

	/**
	 * A {@link SmartMessageConverter} that delegates to another {@link SmartMessageConverter} for conversion.
	 *
	 * Will wrap the returning result of the conversion into a {@link Message} if it is not a {@link Message}
	 * instance already.
	 */
	private final class MessageWrappingMessageConverter implements SmartMessageConverter {

		private final MimeType contentType;

		private final SmartMessageConverter delegate;

		private MessageWrappingMessageConverter(SmartMessageConverter delegate, MimeType contentType) {
			Assert.notNull(delegate, "Delegate converter cannot be null");
			Assert.notNull(contentType, "Content type cannot be null");
			this.delegate = delegate;
			this.contentType = contentType;
		}

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			Object converted = delegate.fromMessage(message, targetClass);
			if (converted instanceof Message) {
				return converted;
			}
			else {
				return build(converted, message.getHeaders());
			}
		}

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
			Object converted = delegate.fromMessage(message, targetClass, conversionHint);
			if (converted == null || converted instanceof Message) {
				return converted;
			}
			else {
				return build(converted, message.getHeaders());
			}
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			return delegate.toMessage(payload, headers);
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
			return delegate.toMessage(payload, headers, conversionHint);
		}

		/**
		 * Convenience method to construct a converted message
		 * @param payload the converted payload
		 * @param headers the existing message headers
		 * @return the converted message
		 */
		protected Object build(Object payload, MessageHeaders headers) {
			MimeType messageContentType = MessageConverterUtils.X_JAVA_OBJECT.equals(contentType) ?
					MessageConverterUtils.javaObjectMimeType(payload.getClass()) : contentType;
			return messageBuilderFactory.withPayload(payload).copyHeaders(headers)
					.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
							messageContentType.toString()))
					.build();
		}
	}
}
