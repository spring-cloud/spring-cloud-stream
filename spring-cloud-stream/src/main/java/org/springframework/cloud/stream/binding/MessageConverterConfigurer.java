/*
 * Copyright 2015-2017 the original author or authors.
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

import java.nio.charset.StandardCharsets;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link MessageChannelConfigurer} that sets data types and message converters based on
 * {@link org.springframework.cloud.stream.config.BindingProperties#contentType}. Also
 * adds a {@link org.springframework.messaging.support.ChannelInterceptor} to the message
 * channel to set the `ContentType` header for the message (if not already set) based on
 * the `ContentType` binding property of the channel.
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Maxim Kirilov
 * @author Gary Russell
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 */
public class MessageConverterConfigurer
		implements MessageChannelConfigurer, BeanFactoryAware, InitializingBean {

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final BindingServiceProperties bindingServiceProperties;

	private ConfigurableListableBeanFactory beanFactory;

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		Assert.notNull(compositeMessageConverterFactory,
				"The message converter factory cannot be null");
		this.bindingServiceProperties = bindingServiceProperties;
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
	public void configureOutputChannel(MessageChannel messageChannel,
			String channelName) {
		configureMessageChannel(messageChannel, channelName, false);
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	private void configureMessageChannel(MessageChannel channel, String channelName,
			boolean input) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		final BindingProperties bindingProperties = this.bindingServiceProperties
				.getBindingProperties(channelName);
		String contentType = bindingProperties.getContentType();
		ProducerProperties producerProperties = bindingProperties.getProducer();
		if (!input && producerProperties != null && producerProperties.isPartitioned()) {
			messageChannel.addInterceptor(new PartitioningInterceptor(bindingProperties,
					getPartitionKeyExtractorStrategy(producerProperties),
					getPartitionSelectorStrategy(producerProperties)));
		}
		if (input) {
			messageChannel.addInterceptor(new InbondMessageConvertingInterceprtor());
		}
		// TODO: Set all interceptors in the correct order for input/output channels
		if (StringUtils.hasText(contentType)) {
			messageChannel.addInterceptor(
					new ContentTypeConvertingInterceptor(contentType, input));
		}
	}

	private PartitionKeyExtractorStrategy getPartitionKeyExtractorStrategy(
			ProducerProperties producerProperties) {
		if (producerProperties.getPartitionKeyExtractorClass() != null) {
			return getBean(producerProperties.getPartitionKeyExtractorClass().getName(),
					PartitionKeyExtractorStrategy.class);
		}
		return null;
	}

	private PartitionSelectorStrategy getPartitionSelectorStrategy(
			ProducerProperties producerProperties) {
		if (producerProperties.getPartitionSelectorClass() != null) {
			return getBean(producerProperties.getPartitionSelectorClass().getName(),
					PartitionSelectorStrategy.class);
		}
		return new DefaultPartitionSelector();
	}

	@SuppressWarnings("unchecked")
	private <T> T getBean(String className, Class<T> type) {
		if (this.beanFactory.containsBean(className)) {
			return this.beanFactory.getBean(className, type);
		}
		else {
			synchronized (this) {
				T bean;
				Class<?> clazz;
				try {
					clazz = ClassUtils.forName(className,
							this.beanFactory.getBeanClassLoader());
				}
				catch (Exception e) {
					throw new BinderException("Failed to load class: " + className, e);
				}
				try {
					bean = (T) clazz.newInstance();
					Assert.isInstanceOf(type, bean);
					this.beanFactory.registerSingleton(className, bean);
					this.beanFactory.initializeBean(bean, className);
				}
				catch (Exception e) {
					throw new BinderException("Failed to instantiate class: " + className,
							e);
				}
				return bean;
			}
		}
	}

	/**
	 * Default partition strategy; only works on keys with "real" hash codes, such as
	 * String. Caller now always applies modulo so no need to do so here.
	 */
	private static class DefaultPartitionSelector implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			int hashCode = key.hashCode();
			if (hashCode == Integer.MIN_VALUE) {
				hashCode = 0;
			}
			return Math.abs(hashCode);
		}

	}

	private final class ContentTypeConvertingInterceptor
			extends ChannelInterceptorAdapter {

		private final MimeType mimeType;

		private final boolean input;

		private final MessageConverter messageConverter;

		private ContentTypeConvertingInterceptor(String contentType, boolean input) {
			this.mimeType = MessageConverterUtils.getMimeType(contentType);
			this.input = input;

			this.messageConverter = MessageConverterConfigurer.this.compositeMessageConverterFactory
					.getMessageConverterForAllRegistered();
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			// bypass conversion for ErrorMessges
			if (message instanceof ErrorMessage) {
				return message;
			}

			Message<?> sentMessage = message;
			Object converted;
			// bypass conversion for raw bytes or input channels
			if (this.input || message.getPayload() instanceof byte[]) {
				return MessageConverterConfigurer.this.messageBuilderFactory
						.withPayload(message.getPayload())
						.copyHeaders(message.getHeaders())
						.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, this.mimeType)
						.build();
			}
			else {
				MutableMessageHeaders headers = new MutableMessageHeaders(
						message.getHeaders());
				if (!headers.containsKey(MessageHeaders.CONTENT_TYPE)) {
					headers.put(MessageHeaders.CONTENT_TYPE, this.mimeType);
				}
				converted = this.messageConverter.toMessage(message.getPayload(),
						headers);
			}
			if (converted != null) {
				if (converted instanceof Message) {
					sentMessage = (Message<?>) converted;
				}
				else {
					sentMessage = MessageConverterConfigurer.this.messageBuilderFactory
							.withPayload(converted).copyHeaders(message.getHeaders())
							.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, this.mimeType)
							.build();
				}
			}
			return sentMessage;
		}
	}

	protected final class PartitioningInterceptor extends ChannelInterceptorAdapter {

		private final BindingProperties bindingProperties;

		private final PartitionHandler partitionHandler;

		PartitioningInterceptor(BindingProperties bindingProperties,
				PartitionKeyExtractorStrategy partitionKeyExtractorStrategy,
				PartitionSelectorStrategy partitionSelectorStrategy) {
			this.bindingProperties = bindingProperties;
			this.partitionHandler = new PartitionHandler(
					ExpressionUtils.createStandardEvaluationContext(
							MessageConverterConfigurer.this.beanFactory),
					this.bindingProperties.getProducer(), partitionKeyExtractorStrategy,
					partitionSelectorStrategy);
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			if (!message.getHeaders().containsKey(BinderHeaders.PARTITION_OVERRIDE)) {
				int partition = this.partitionHandler.determinePartition(message);
				return MessageConverterConfigurer.this.messageBuilderFactory
						.fromMessage(message)
						.setHeader(BinderHeaders.PARTITION_HEADER, partition).build();
			}
			else {
				return MessageConverterConfigurer.this.messageBuilderFactory
						.fromMessage(message)
						.setHeader(BinderHeaders.PARTITION_HEADER,
								message.getHeaders()
										.get(BinderHeaders.PARTITION_OVERRIDE))
						.removeHeader(BinderHeaders.PARTITION_OVERRIDE).build();
			}
		}
	}

	public final static class InbondMessageConvertingInterceprtor extends ChannelInterceptorAdapter {

		private final DefaultContentTypeResolver contentTypeResolver = new DefaultContentTypeResolver();
		private final CompositeMessageConverterFactory converterFactory = new CompositeMessageConverterFactory();

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			Class<?> targetClass = null;
			MessageConverter converter = null;
			MimeType contentType = message.getHeaders().containsKey(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)
						? MimeType.valueOf((String)message.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE))
								: contentTypeResolver.resolve(message.getHeaders());

			if (contentType != null){
				if (equalTypeAndSubType(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, contentType) || equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT, contentType)){
					// for Java and Kryo de-serialization we need to reset the content type
					message = MessageBuilder.fromMessage(message).setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
					converter = equalTypeAndSubType(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, contentType)
							? converterFactory.getMessageConverterForType(contentType)
									: converterFactory.getMessageConverterForAllRegistered();
					String targetClassName = contentType.getParameter("type");
					if (StringUtils.hasText(targetClassName)) {
						try {
							targetClass = Class.forName(targetClassName, false, Thread.currentThread().getContextClassLoader());
						}
						catch (Exception e) {
							throw new IllegalStateException("Failed to determine class name for contentType: "
									+ message.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE), e);
						}
					}
				}
			}

			Object payload;
			if (converter != null){
				Assert.isTrue(!(equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT, contentType) && targetClass == null),
						"Can not deserialize into message since 'contentType` has not "
							+ "being encoded with the actual target type."
							+ "Consider 'application/x-java-object; type=foo.bar.MyClass'");
				payload = converter.fromMessage(message, targetClass);
			}
			else {
				MimeType deserializeContentType = contentTypeResolver.resolve(message.getHeaders());
				deserializeContentType = deserializeContentType == null ? contentType : deserializeContentType;
				payload = deserializeContentType == null ? message.getPayload() : this.deserializePayload(message.getPayload(), deserializeContentType);
			}
			message = MessageBuilder.withPayload(payload)
					.copyHeaders(message.getHeaders())
					.setHeader(MessageHeaders.CONTENT_TYPE, contentType)
					.removeHeader(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)
					.build();
			return message;
		}

		private Object deserializePayload(Object payload, MimeType contentType) {
			if (payload instanceof byte[] && ("text".equalsIgnoreCase(contentType.getType()) || equalTypeAndSubType(MimeTypeUtils.APPLICATION_JSON, contentType))) {
				payload = new String((byte[])payload, StandardCharsets.UTF_8);
			}
			return payload;
		}
	}

	/*
	 * Candidate to go into some utils class
	 */
	private static boolean equalTypeAndSubType(MimeType m1, MimeType m2) {
		return m1 != null && m2 != null && m1.getType().equalsIgnoreCase(m2.getType()) && m1.getSubtype().equalsIgnoreCase(m2.getSubtype());
	}

}
