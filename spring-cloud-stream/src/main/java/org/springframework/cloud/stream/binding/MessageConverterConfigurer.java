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


import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.tuple.Tuple;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * A {@link MessageChannelConfigurer} that sets data types and message converters based on {@link
 * org.springframework.cloud.stream.config.BindingProperties#contentType}. Also adds a
 * {@link org.springframework.messaging.support.ChannelInterceptor} to
 * the message channel to set the `ContentType` header for the message (if not already set) based on the `ContentType`
 * binding property of the channel.
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
public class MessageConverterConfigurer implements MessageChannelConfigurer, BeanFactoryAware, InitializingBean {

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	private ConfigurableListableBeanFactory beanFactory;

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	public MessageConverterConfigurer(ChannelBindingServiceProperties channelBindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		Assert.notNull(compositeMessageConverterFactory, "The message converter factory cannot be null");
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

	@Override
	public void configureInputChannel(MessageChannel messageChannel, String channelName) {
		configureMessageChannel(messageChannel, channelName, true);
	}

	@Override
	public void configureOutputChannel(MessageChannel messageChannel, String channelName) {
		configureMessageChannel(messageChannel, channelName, false);
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 * @param channel     message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	private void configureMessageChannel(MessageChannel channel, String channelName, boolean input) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		final BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindingProperties(
				channelName);
		final String contentType = bindingProperties.getContentType();
		if (!input && bindingProperties.getProducer() != null && bindingProperties.getProducer().isPartitioned()) {
			messageChannel.addInterceptor(new PartitioningInterceptor(bindingProperties));
		}
		if (StringUtils.hasText(contentType)) {
			messageChannel.addInterceptor(new ContentTypeConvertingInterceptor(contentType, input));
		}
	}


	private final class ContentTypeConvertingInterceptor extends ChannelInterceptorAdapter {

		private final String contentType;

		private final MimeType mimeType;

		private final boolean input;

		private final Class<?> klazz;

		private final MessageConverter messageConverter;

		private final boolean provideHint;

		private ContentTypeConvertingInterceptor(String contentType, boolean input) {
			this.contentType = contentType;
			this.mimeType = MessageConverterUtils.getMimeType(contentType);
			this.input = input;
			if (MessageConverterUtils.X_JAVA_OBJECT.equals(this.mimeType)) {
				this.klazz =
						MessageConverterUtils
								.getJavaTypeForJavaObjectContentType(this.mimeType);
			}
			else if (this.mimeType.equals(MessageConverterUtils.X_SPRING_TUPLE)) {
				this.klazz = Tuple.class;
			}
			else if (this.mimeType.getType().equals("text") || this.mimeType.getSubtype().equals(
					"json") || this.mimeType.getSubtype().equals("xml")) {
				this.klazz = String.class;
			}
			else {
				this.klazz = byte[].class;
			}
			this.messageConverter =
					MessageConverterConfigurer.this.compositeMessageConverterFactory
							.getMessageConverterForType(this.mimeType);
			this.provideHint = this.messageConverter instanceof AbstractMessageConverter;
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			Message<?> sentMessage = null;
			if (this.klazz.isAssignableFrom(message.getPayload().getClass())) {
				Object contentTypeFromMessage = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
				if (contentTypeFromMessage == null) {
					sentMessage = MessageConverterConfigurer.this.messageBuilderFactory
							.fromMessage(message)
							.setHeader(MessageHeaders.CONTENT_TYPE, this.contentType)
							.build();
				}
				else {
					sentMessage = message;
				}
			}
			else {
				Object converted;
				if (this.input) {
					if (this.provideHint) {
						converted = ((AbstractMessageConverter) this.messageConverter).fromMessage(message, this.klazz,
								this.mimeType);
					}
					else {
						converted = this.messageConverter.fromMessage(message, this.klazz);
					}
				}
				else {
					if (this.provideHint) {
						converted = ((AbstractMessageConverter) this.messageConverter).toMessage(message.getPayload(),
								new MutableMessageHeaders(message.getHeaders()), this.mimeType);
					}
					else {
						converted = this.messageConverter.toMessage(message.getPayload(),
								new MutableMessageHeaders(message.getHeaders()));
					}
				}
				if (converted instanceof Message) {
					sentMessage = (Message<?>) converted;
				}
				else {
					sentMessage = MessageConverterConfigurer.this.messageBuilderFactory.withPayload(converted)
							.copyHeaders(message.getHeaders()).setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE,

									this.mimeType).build();
				}
			}
			if (sentMessage == null) {
				throw new MessageConversionException("Cannot convert " + message + " to " + this.contentType);
			}
			return sentMessage;
		}
	}

	private final class PartitioningInterceptor extends ChannelInterceptorAdapter {

		private final BindingProperties bindingProperties;

		private PartitioningInterceptor(BindingProperties bindingProperties) {
			this.bindingProperties = bindingProperties;
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			EvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(
					MessageConverterConfigurer.this.beanFactory);
			PartitionHandler partitionHandler = new PartitionHandler(MessageConverterConfigurer.this
					.beanFactory, evaluationContext, null,
					this.bindingProperties.getProducer());
			int partition = partitionHandler.determinePartition(message);
			return new MutableMessageBuilderFactory().fromMessage(message)
					.setHeader(BinderHeaders.PARTITION_HEADER, partition).build();

		}
	}
}
