/*
 * Copyright 2015-2018 the original author or authors.
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
import java.util.Collections;
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
import org.springframework.cloud.stream.binder.MessageValues;
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
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ObjectUtils;
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
public class MessageConverterConfigurer implements MessageChannelAndSourceConfigurer, BeanFactoryAware {

	private final Log logger = LogFactory.getLog(getClass());

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final BindingServiceProperties bindingServiceProperties;

	private ConfigurableListableBeanFactory beanFactory;

	private final Map<String, PartitionKeyExtractorStrategy> partitionKeyExtractors;

	private final Map<String, PartitionSelectorStrategy> partitionSelectors;

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this(bindingServiceProperties, compositeMessageConverterFactory, Collections.emptyMap(), Collections.emptyMap());
	}

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory,
			Map<String, PartitionKeyExtractorStrategy> partitionKeyExtractors,
			Map<String, PartitionSelectorStrategy> partitionSelectors) {
		Assert.notNull(compositeMessageConverterFactory,
				"The message converter factory cannot be null");
		this.bindingServiceProperties = bindingServiceProperties;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
		this.partitionKeyExtractors = partitionKeyExtractors == null ? Collections.emptyMap() : partitionKeyExtractors;
		this.partitionSelectors = partitionSelectors == null ? Collections.emptyMap() : partitionSelectors;
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
		BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(name);
		String contentType = bindingProperties.getContentType();
		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if ((consumerProperties == null || !consumerProperties.isUseNativeDecoding())
				&& binding instanceof DefaultPollableMessageSource) {
			((DefaultPollableMessageSource) binding).addInterceptor(
					new InboundContentTypeConvertingInterceptor(contentType, this.compositeMessageConverterFactory));
		}
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 * @param inbound inbound (i.e., "input") or outbound channel
	 */
	private void configureMessageChannel(MessageChannel channel, String channelName, boolean inbound) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(channelName);
		String contentType = bindingProperties.getContentType();
		ProducerProperties producerProperties = bindingProperties.getProducer();
		if (!inbound && producerProperties != null && producerProperties.isPartitioned()) {
			messageChannel.addInterceptor(new PartitioningInterceptor(bindingProperties,
					getPartitionKeyExtractorStrategy(producerProperties),
					getPartitionSelectorStrategy(producerProperties)));
		}

		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if (this.isNativeEncodingNotSet(producerProperties, consumerProperties, inbound)) {
			if (inbound) {
				messageChannel.addInterceptor(new InboundContentTypeConvertingInterceptor(contentType, this.compositeMessageConverterFactory));
			}
			else {
				messageChannel.addInterceptor(new OutboundContentTypeConvertingInterceptor(contentType, this.compositeMessageConverterFactory
						.getMessageConverterForAllRegistered()));
			}
		}
	}

	private boolean isNativeEncodingNotSet(ProducerProperties producerProperties, ConsumerProperties consumerProperties, boolean input) {
		if (input) {
			return consumerProperties == null || !consumerProperties.isUseNativeDecoding();
		}
		else {
			return producerProperties == null || !producerProperties.isUseNativeEncoding();
		}
	}

	@SuppressWarnings("deprecation")
	private PartitionKeyExtractorStrategy getPartitionKeyExtractorStrategy(ProducerProperties producerProperties) {
		PartitionKeyExtractorStrategy partitionKeyExtractor;
		if (producerProperties.getPartitionKeyExtractorClass() != null) {
			logger.warn("'partitionKeyExtractorClass' option is deprecated as of v2.0. Please configure partition "
					+ "key extractor as a @Bean that implements 'PartitionKeyExtractorStrategy'. Additionally you can "
					+ "specify 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' to specify which "
					+ "bean to use in the event there are more then one.");
			partitionKeyExtractor = instantiate(producerProperties.getPartitionKeyExtractorClass(), PartitionKeyExtractorStrategy.class);
		}
		else if (StringUtils.hasText(producerProperties.getPartitionKeyExtractorName())) {
			partitionKeyExtractor = this.partitionKeyExtractors.get(producerProperties.getPartitionKeyExtractorName());
			Assert.notNull(partitionKeyExtractor, "PartitionKeyExtractorStrategy bean with the name '" + producerProperties.getPartitionKeyExtractorName()
				+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Assert.isTrue(CollectionUtils.isEmpty(this.partitionKeyExtractors) || this.partitionKeyExtractors.size() <= 1,
					"Multiple  beans of type 'PartitionKeyExtractorStrategy' found. " + this.partitionKeyExtractors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' property to specify "
							+ "the name of the bean to be used.");
			partitionKeyExtractor = CollectionUtils.isEmpty(this.partitionKeyExtractors) ?
					null : this.partitionKeyExtractors.values().iterator().next();
		}
		return partitionKeyExtractor;
	}

	@SuppressWarnings("deprecation")
	private PartitionSelectorStrategy getPartitionSelectorStrategy(ProducerProperties producerProperties) {
		PartitionSelectorStrategy partitionSelector;
		if (producerProperties.getPartitionSelectorClass() != null) {
			logger.warn("'partitionSelectorClass' option is deprecated as of v2.0. Please configure partition "
					+ "selector as a @Bean that implements 'PartitionSelectorStrategy'. Additionally you can "
					+ "specify 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' to specify which "
					+ "bean to use in the event there are more then one.");
			partitionSelector = instantiate(producerProperties.getPartitionSelectorClass(), PartitionSelectorStrategy.class);
		}
		else if (StringUtils.hasText(producerProperties.getPartitionSelectorName())) {
			partitionSelector = this.partitionSelectors.get(producerProperties.getPartitionSelectorName());
			Assert.notNull(partitionSelector, "PartitionSelectorStrategy bean with the name '" + producerProperties.getPartitionSelectorName()
				+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Assert.isTrue(CollectionUtils.isEmpty(this.partitionSelectors) || this.partitionSelectors.size() <= 1,
					"Multiple  beans of type 'PartitionSelectorStrategy' found. " + this.partitionSelectors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' property to specify "
							+ "the name of the bean to be used.");
			partitionSelector = CollectionUtils.isEmpty(this.partitionSelectors)
					? new DefaultPartitionSelector() : this.partitionSelectors.values().iterator().next();
		}
		return partitionSelector;
	}

	@SuppressWarnings("unchecked")
	private <T> T instantiate(Class<?> implClass, Class<T> type) {
		try {
			return (T) implClass.newInstance();
		}
		catch (Exception e) {
			throw new BinderException("Failed to instantiate class: " + implClass.getName(), e);
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
	 * Primary purpose of this interceptor is to enhance/enrich Message that sent to the *inbound*
	 * channel with 'contentType' header for cases where 'contentType' is not present in the Message
	 * itself but set on such channel via {@link BindingProperties#setContentType(String)}.
	 * <br>
	 * Secondary purpose of this interceptor is to provide backward compatibility with previous versions of SCSt
	 * to support some of the type conversion assumptions.
	 * See InboundContentTypeConvertingInterceptor.deserializePayload(..) for more details.
	 */
	private final class InboundContentTypeConvertingInterceptor extends ChannelInterceptorAdapter {

		private final MimeType mimeType;

		private final CompositeMessageConverterFactory compositeMessageConverterFactory;

		private InboundContentTypeConvertingInterceptor(String contentType, CompositeMessageConverterFactory compositeMessageConverterFactory) {
			this.mimeType = MessageConverterUtils.getMimeType(contentType);
			this.compositeMessageConverterFactory = compositeMessageConverterFactory;
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			if (message instanceof ErrorMessage) {
				return message;
			}

			Message<?> postProcessedMessage = message;
			MimeType contentType = this.mimeType;
			if (message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)) {
				Object ct = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
				contentType = ct instanceof String ? MimeType.valueOf((String)ct) : (MimeType)ct;
			}

			boolean deserializationRequired = message.getPayload() instanceof byte[] &&
					("text".equalsIgnoreCase(contentType.getType()) ||
					equalTypeAndSubType(MimeTypeUtils.APPLICATION_JSON, contentType) ||
					equalTypeAndSubType(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, contentType) ||
					equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT, contentType));

			Object payload = deserializationRequired ? this.deserializePayload(message, contentType) : message.getPayload();

			if (payload != null) {
				Object ct = message.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
				contentType = ct instanceof String ? MimeType.valueOf((String)ct) : (ct == null ? contentType : (MimeType)ct);
				postProcessedMessage = MessageConverterConfigurer.this.messageBuilderFactory
						.withPayload(payload)
						.copyHeaders(message.getHeaders())
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType)
						.removeHeader(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)
						.build();
			}
			return postProcessedMessage;
		}


		/**
		 * Will *only* deserialize payload if its 'contentType' is 'text/* or application/json' or Java/Kryo serialized.
		 * While this would naturally happen via MessageConverters at the time of handler method
		 * invocation, doing it here also is strictly to support behavior established
		 * in previous versions of SCSt. One of these cases is return payload as String if contentType is text or json.
		 * Also to support certain type of assumptions on type-less handlers (i.e., handle(?) vs. handle(Foo));
		 */
		private Object deserializePayload(Message<?> message, MimeType contentTypeToUse) {
			Object payload = null;

			if ("text".equalsIgnoreCase(contentTypeToUse.getType()) || equalTypeAndSubType(MimeTypeUtils.APPLICATION_JSON, contentTypeToUse)) {
				payload = new String((byte[])message.getPayload(), StandardCharsets.UTF_8);
			}
			else {
				message = MessageBuilder.fromMessage(message).setHeader(MessageHeaders.CONTENT_TYPE, contentTypeToUse).build();
				MessageConverter converter = equalTypeAndSubType(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, contentTypeToUse)
						? compositeMessageConverterFactory.getMessageConverterForType(contentTypeToUse)
								: compositeMessageConverterFactory.getMessageConverterForAllRegistered();
				String targetClassName = contentTypeToUse.getParameter("type");
				Class<?> targetClass = null;
				if (StringUtils.hasText(targetClassName)) {
					try {
						targetClass = Class.forName(targetClassName, false, Thread.currentThread().getContextClassLoader());
					}
					catch (Exception e) {
						throw new IllegalStateException("Failed to determine class name for contentType: "
								+ message.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE), e);
					}
				}

				Assert.isTrue(!(equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT, contentTypeToUse) && targetClass == null),
						"Cannot deserialize into message since 'contentType` is not "
							+ "encoded with the actual target type."
							+ "Consider 'application/x-java-object; type=foo.bar.MyClass'");
				payload = converter.fromMessage(message, targetClass);
			}
			return payload;
		}
		/*
		 * Candidate to go into some utils class
		 */
		private boolean equalTypeAndSubType(MimeType m1, MimeType m2) {
			return m1 != null && m2 != null && m1.getType().equalsIgnoreCase(m2.getType()) && m1.getSubtype().equalsIgnoreCase(m2.getSubtype());
		}
	}

	/**
	 * Unlike INBOUND where the target type is known and conversion is typically done by argument
	 * resolvers of {@link InvocableHandlerMethod} for the OUTBOUND case it is not known so we simply
	 * rely on provided MessageConverters that will use the provided 'contentType' and convert messages
	 * to a type dictated by the Binders (i.e., byte[]).
	 */
	private final class OutboundContentTypeConvertingInterceptor extends ChannelInterceptorAdapter {

		private final MimeType mimeType;

		private final MessageConverter messageConverter;

		private OutboundContentTypeConvertingInterceptor(String contentType, CompositeMessageConverter messageConverter) {
			this.mimeType = MessageConverterUtils.getMimeType(contentType);
			this.messageConverter = messageConverter;
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			Message<?> postProcessedMessage = message;
			if (!(message instanceof ErrorMessage)) {
				MutableMessageHeaders headers = new MutableMessageHeaders(message.getHeaders());
				headers.putIfAbsent(MessageHeaders.CONTENT_TYPE, this.mimeType);
				Message<?> converted = this.messageConverter.toMessage(message.getPayload(), headers);
				if (converted != null) {
					postProcessedMessage = converted;
				} else {
					postProcessedMessage = MessageConverterConfigurer.this.messageBuilderFactory
					.withPayload(message.getPayload())
					.copyHeaders(headers)
					.build();
				}
				postProcessedMessage = this.finishPreSend(postProcessedMessage);
			}
			return postProcessedMessage;
		}

		/**
		 * This is strictly to support 1.3 semantics where BINDER_ORIGINAL_CONTENT_TYPE header
		 * needs to be set for certain cases and String payloads needs to be converted to byte[].
		 *
		 * Factored out of what was left of MessageSerializationUtils.
		 */
		// deprecated at the get go as a reminder to remove at v3.0
		@Deprecated
		private Message<?> finishPreSend(Message<?> message) {
			String oct = message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE) ? message.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString() : null;
			String ct = oct;
			if (message.getPayload() instanceof String) {
				ct = JavaClassMimeTypeUtils.mimeTypeFromObject(message.getPayload(), ObjectUtils.nullSafeToString(oct)).toString();
			}
			MessageValues messageValues = new MessageValues(message);
			Object payload = message.getPayload();
			if (payload instanceof String) {
				payload = ((String)payload).getBytes(StandardCharsets.UTF_8);
			}

			messageValues.setPayload(payload);
			if (ct != null && !ct.equals(oct)) {
				messageValues.put(MessageHeaders.CONTENT_TYPE, ct);
				messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, oct);
			}
			return messageValues.toMessage();
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
}
