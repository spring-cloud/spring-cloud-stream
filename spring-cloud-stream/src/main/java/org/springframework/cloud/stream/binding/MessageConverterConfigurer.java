/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.JavaClassMimeTypeUtils;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
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
		implements MessageChannelAndSourceConfigurer, BeanFactoryAware {

	private final Log logger = LogFactory.getLog(getClass());

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final BindingServiceProperties bindingServiceProperties;

	private final Field headersField;

	private ConfigurableListableBeanFactory beanFactory;

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		Assert.notNull(compositeMessageConverterFactory,
				"The message converter factory cannot be null");
		this.bindingServiceProperties = bindingServiceProperties;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;

		this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
		this.headersField.setAccessible(true);
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
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

	@Override
	public void configurePolledMessageSource(PollableMessageSource binding, String name) {
		BindingProperties bindingProperties = this.bindingServiceProperties
				.getBindingProperties(name);
		String contentType = bindingProperties.getContentType();
		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if ((consumerProperties == null || !consumerProperties.isUseNativeDecoding())
				&& binding instanceof DefaultPollableMessageSource) {
			((DefaultPollableMessageSource) binding).addInterceptor(
					new InboundContentTypeEnhancingInterceptor(contentType));
		}
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 * @param inbound inbound (i.e., "input") or outbound channel
	 */
	private void configureMessageChannel(MessageChannel channel, String channelName,
			boolean inbound) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		BindingProperties bindingProperties = this.bindingServiceProperties
				.getBindingProperties(channelName);
		String contentType = bindingProperties.getContentType();
		ProducerProperties producerProperties = bindingProperties.getProducer();
		if (!inbound && producerProperties != null
				&& producerProperties.isPartitioned()) {
			messageChannel.addInterceptor(new PartitioningInterceptor(bindingProperties,
					getPartitionKeyExtractorStrategy(producerProperties),
					getPartitionSelectorStrategy(producerProperties)));
		}

		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if (this.isNativeEncodingNotSet(producerProperties, consumerProperties,
				inbound)) {
			if (inbound) {
				messageChannel.addInterceptor(
						new InboundContentTypeEnhancingInterceptor(contentType));
			}
			else {
				messageChannel.addInterceptor(
						new OutboundContentTypeConvertingInterceptor(contentType,
								this.compositeMessageConverterFactory
										.getMessageConverterForAllRegistered()));
			}
		}
	}

	private boolean isNativeEncodingNotSet(ProducerProperties producerProperties,
			ConsumerProperties consumerProperties, boolean input) {
		if (input) {
			return consumerProperties == null
					|| !consumerProperties.isUseNativeDecoding();
		}
		else {
			return producerProperties == null
					|| !producerProperties.isUseNativeEncoding();
		}
	}

	@SuppressWarnings("deprecation")
	private PartitionKeyExtractorStrategy getPartitionKeyExtractorStrategy(
			ProducerProperties producerProperties) {
		PartitionKeyExtractorStrategy partitionKeyExtractor;
		if (producerProperties.getPartitionKeyExtractorClass() != null) {
			this.logger.warn(
					"'partitionKeyExtractorClass' option is deprecated as of v2.0. Please configure partition "
							+ "key extractor as a @Bean that implements 'PartitionKeyExtractorStrategy'. Additionally you can "
							+ "specify 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' to specify which "
							+ "bean to use in the event there are more then one.");
			partitionKeyExtractor = instantiate(
					producerProperties.getPartitionKeyExtractorClass(),
					PartitionKeyExtractorStrategy.class);
		}
		else if (StringUtils.hasText(producerProperties.getPartitionKeyExtractorName())) {
			partitionKeyExtractor = this.beanFactory.getBean(
					producerProperties.getPartitionKeyExtractorName(),
					PartitionKeyExtractorStrategy.class);
			Assert.notNull(partitionKeyExtractor,
					"PartitionKeyExtractorStrategy bean with the name '"
							+ producerProperties.getPartitionKeyExtractorName()
							+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Map<String, PartitionKeyExtractorStrategy> extractors = this.beanFactory
					.getBeansOfType(PartitionKeyExtractorStrategy.class);
			Assert.isTrue(extractors.size() <= 1,
					"Multiple  beans of type 'PartitionKeyExtractorStrategy' found. "
							+ extractors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' property to specify "
							+ "the name of the bean to be used.");
			partitionKeyExtractor = CollectionUtils.isEmpty(extractors) ? null
					: extractors.values().iterator().next();
		}
		return partitionKeyExtractor;
	}

	@SuppressWarnings("deprecation")
	private PartitionSelectorStrategy getPartitionSelectorStrategy(
			ProducerProperties producerProperties) {
		PartitionSelectorStrategy partitionSelector;
		if (producerProperties.getPartitionSelectorClass() != null) {
			this.logger.warn(
					"'partitionSelectorClass' option is deprecated as of v2.0. Please configure partition "
							+ "selector as a @Bean that implements 'PartitionSelectorStrategy'. Additionally you can "
							+ "specify 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' to specify which "
							+ "bean to use in the event there are more then one.");
			partitionSelector = instantiate(
					producerProperties.getPartitionSelectorClass(),
					PartitionSelectorStrategy.class);
		}
		else if (StringUtils.hasText(producerProperties.getPartitionSelectorName())) {
			partitionSelector = this.beanFactory.getBean(
					producerProperties.getPartitionSelectorName(),
					PartitionSelectorStrategy.class);
			Assert.notNull(partitionSelector,
					"PartitionSelectorStrategy bean with the name '"
							+ producerProperties.getPartitionSelectorName()
							+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Map<String, PartitionSelectorStrategy> selectors = this.beanFactory
					.getBeansOfType(PartitionSelectorStrategy.class);
			Assert.isTrue(selectors.size() <= 1,
					"Multiple  beans of type 'PartitionSelectorStrategy' found. "
							+ selectors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' property to specify "
							+ "the name of the bean to be used.");
			partitionSelector = CollectionUtils.isEmpty(selectors)
					? new DefaultPartitionSelector()
					: selectors.values().iterator().next();
		}
		return partitionSelector;
	}

	@SuppressWarnings("unchecked")
	private <T> T instantiate(Class<?> implClass, Class<T> type) {
		try {
			return (T) implClass.newInstance();
		}
		catch (Exception e) {
			throw new BinderException(
					"Failed to instantiate class: " + implClass.getName(), e);
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

	/**
	 * Primary purpose of this interceptor is to enhance/enrich Message that sent to the
	 * *inbound* channel with 'contentType' header for cases where 'contentType' is not
	 * present in the Message itself but set on such channel via
	 * {@link BindingProperties#setContentType(String)}. <br>
	 * Secondary purpose of this interceptor is to provide backward compatibility with
	 * previous versions of SCSt.
	 */
	private final class InboundContentTypeEnhancingInterceptor
			extends AbstractContentTypeInterceptor {

		private InboundContentTypeEnhancingInterceptor(String contentType) {
			super(contentType);
		}

		@Override
		public Message<?> doPreSend(Message<?> message, MessageChannel channel) {
			@SuppressWarnings("unchecked")
			Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
					.getField(MessageConverterConfigurer.this.headersField,
							message.getHeaders());
			MimeType contentType = this.mimeType;
			/*
			 * NOTE: The below code for BINDER_ORIGINAL_CONTENT_TYPE is to support legacy
			 * message format established in 1.x version of the framework and should/will
			 * no longer be supported in 3.x
			 */
			if (message.getHeaders()
					.containsKey(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)) {
				Object ct = message.getHeaders()
						.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
				contentType = ct instanceof String ? MimeType.valueOf((String) ct)
						: (ct == null ? this.mimeType : (MimeType) ct);
				headersMap.put(MessageHeaders.CONTENT_TYPE, contentType);
				headersMap.remove(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			}
			// == end legacy note

			if (!message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)) {
				headersMap.put(MessageHeaders.CONTENT_TYPE, contentType);
			}
			else if (message.getHeaders()
					.get(MessageHeaders.CONTENT_TYPE) instanceof String) {
				headersMap.put(MessageHeaders.CONTENT_TYPE, MimeType.valueOf(
						(String) message.getHeaders().get(MessageHeaders.CONTENT_TYPE)));
			}

			return message;
		}

	}

	/**
	 * Unlike INBOUND where the target type is known and conversion is typically done by
	 * argument resolvers of {@link InvocableHandlerMethod} for the OUTBOUND case it is
	 * not known so we simply rely on provided MessageConverters that will use the
	 * provided 'contentType' and convert messages to a type dictated by the Binders
	 * (i.e., byte[]).
	 */
	private final class OutboundContentTypeConvertingInterceptor
			extends AbstractContentTypeInterceptor {

		private final MessageConverter messageConverter;

		private OutboundContentTypeConvertingInterceptor(String contentType,
				CompositeMessageConverter messageConverter) {
			super(contentType);
			this.messageConverter = messageConverter;
		}

		@Override
		public Message<?> doPreSend(Message<?> message, MessageChannel channel) {
			// If handler is a function, FunctionInvoker will already perform message
			// conversion.
			// In fact in the future we should consider propagating knowledge of the
			// default content type
			// to MessageConverters instead of interceptors
			if (message.getPayload() instanceof byte[]
					&& message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)) {
				return message;
			}

			// ===== 1.3 backward compatibility code part-1 ===
			String oct = message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)
					? message.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString()
					: null;
			String ct = message.getPayload() instanceof String
					? JavaClassMimeTypeUtils.mimeTypeFromObject(message.getPayload(),
							ObjectUtils.nullSafeToString(oct)).toString()
					: oct;
			// ===== END 1.3 backward compatibility code part-1 ===

			if (!message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
						.getField(MessageConverterConfigurer.this.headersField,
								message.getHeaders());
				headersMap.put(MessageHeaders.CONTENT_TYPE, this.mimeType);
			}

			@SuppressWarnings("unchecked")
			Message<byte[]> outboundMessage = message.getPayload() instanceof byte[]
					? (Message<byte[]>) message : (Message<byte[]>) this.messageConverter
							.toMessage(message.getPayload(), message.getHeaders());
			if (outboundMessage == null) {
				throw new IllegalStateException("Failed to convert message: '" + message
						+ "' to outbound message.");
			}

			/// ===== 1.3 backward compatibility code part-2 ===
			if (ct != null && !ct.equals(oct) && oct != null) {
				@SuppressWarnings("unchecked")
				Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
						.getField(MessageConverterConfigurer.this.headersField,
								outboundMessage.getHeaders());
				headersMap.put(MessageHeaders.CONTENT_TYPE, MimeType.valueOf(ct));
				headersMap.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE,
						MimeType.valueOf(oct));
			}
			// ===== END 1.3 backward compatibility code part-2 ===
			return outboundMessage;
		}

	}

	/**
	 *
	 */
	private abstract class AbstractContentTypeInterceptor implements ChannelInterceptor {

		final MimeType mimeType;

		private AbstractContentTypeInterceptor(String contentType) {
			this.mimeType = MessageConverterUtils.getMimeType(contentType);
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			return message instanceof ErrorMessage ? message
					: this.doPreSend(message, channel);
		}

		protected abstract Message<?> doPreSend(Message<?> message,
				MessageChannel channel);

	}

	/**
	 * Partitioning channel interceptor.
	 */
	public final class PartitioningInterceptor implements ChannelInterceptor {

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

		public void setPartitionCount(int partitionCount) {
			this.partitionHandler.setPartitionCount(partitionCount);
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

}
