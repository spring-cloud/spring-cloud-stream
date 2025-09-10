/*
 * Copyright 2015-present the original author or authors.
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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.JavaClassMimeTypeUtils;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.function.BindableFunctionProxyFactory;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.env.Environment;
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

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	private final CompositeMessageConverter compositeMessageConverter;

	private final BindingServiceProperties bindingServiceProperties;

	private final Field headersField;

	private final StreamFunctionProperties streamFunctionProperties;

	private ConfigurableListableBeanFactory beanFactory;

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverter compositeMessageConverter, StreamFunctionProperties streamFunctionProperties) {
		Assert.notNull(compositeMessageConverter,
				"The message converter factory cannot be null");
		this.bindingServiceProperties = bindingServiceProperties;
		this.compositeMessageConverter = compositeMessageConverter;

		this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
		this.headersField.setAccessible(true);
		this.streamFunctionProperties = streamFunctionProperties;
	}

	public MessageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverter compositeMessageConverter) {
		this(bindingServiceProperties, compositeMessageConverter, null);
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
		boolean partitioned = !inbound && producerProperties != null && producerProperties.isPartitioned();
		boolean functional = streamFunctionProperties != null
				&& (StringUtils.hasText(streamFunctionProperties.getDefinition())
						|| StringUtils.hasText(bindingServiceProperties.getInputBindings())
						|| StringUtils.hasText(bindingServiceProperties.getOutputBindings()));

		if (partitioned) {
			if (inbound || !functional) {
				messageChannel.addInterceptor(new PartitioningInterceptor(bindingProperties));
			}
		}

		Environment environment = this.beanFactory  == null ? null : this.beanFactory.getBean(Environment.class);
		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if (this.isNativeEncodingNotSet(producerProperties, consumerProperties, inbound)) {
			if (inbound) {
				messageChannel.addInterceptor(new InboundContentTypeEnhancingInterceptor(contentType));
			}
			else {
				if (environment != null && environment.containsProperty("spring.cloud.stream.rabbit.bindings." + channelName + ".producer.routing-key-expression")) {
					Map<String, String> channelNameToFunctions = BindableFunctionProxyFactory.getChannelNameToFunctions();
					String functionName = channelNameToFunctions.get(channelName);
					String outputBindings = environment.getProperty("spring.cloud.stream.output-bindings");
					FunctionInvocationWrapper function = null;
					if (outputBindings != null && outputBindings.contains(functionName)) {
						try {
							function = retrieveFunction(functionName);
						}
						catch (Exception e) {
							// we expect an exception in this case since we didn't have a matching function
							// for the function name due to output-bindings, hence passing through.
							// See https://github.com/spring-cloud/spring-cloud-stream/issues/2921 for more details.
						}
					}
					else {
						function = retrieveFunction(functionName);
					}
					if (function != null) {
						function.setSkipOutputConversion(true);
					}
					functional = false;
				}
				if (!functional) {
					messageChannel.addInterceptor(new OutboundContentTypeConvertingInterceptor(contentType, this.compositeMessageConverter));
				}
			}
		}
	}

	private FunctionInvocationWrapper retrieveFunction(String functionName) {
		FunctionCatalog catalog = this.beanFactory.getBean(FunctionCatalog.class);
		FunctionInvocationWrapper function = catalog.lookup(functionName);
		if (function != null) {
			function.setSkipOutputConversion(true);
		}
		return function;
	}

	private void skipOutputConversionIfNecessary(String functionName) {
		FunctionInvocationWrapper function = retrieveFunction(functionName);
		if (function != null) {
			function.setSkipOutputConversion(true);
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
			if (message.getPayload() instanceof byte[]
					&& message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)) {
				return message;
			}

			String oct = message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)
					? message.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString()
					: null;
			String ct = message.getPayload() instanceof String
					? JavaClassMimeTypeUtils.mimeTypeFromObject(message.getPayload(),
							ObjectUtils.nullSafeToString(oct)).toString()
					: oct;

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

			if (ct != null && !ct.equals(oct) && oct != null) {
				@SuppressWarnings("unchecked")
				Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
						.getField(MessageConverterConfigurer.this.headersField,
								outboundMessage.getHeaders());
				headersMap.put(MessageHeaders.CONTENT_TYPE, MimeType.valueOf(ct));
			}
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

		PartitioningInterceptor(BindingProperties bindingProperties) {
			this.bindingProperties = bindingProperties;
			this.partitionHandler = new PartitionHandler(
					ExpressionUtils.createStandardEvaluationContext(
							MessageConverterConfigurer.this.beanFactory),
					this.bindingProperties.getProducer(), MessageConverterConfigurer.this.beanFactory);
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
