/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.Lifecycle;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.FixedSubscriberChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

/**
 * {@link AbstractBinder} that serves as base class for {@link MessageChannel}
 * binders. Implementors must implement the following methods:
 * <ul>
 * <li>{@link #createProducerDestinationIfNecessary(String, ProducerProperties)}</li>
 * <li>{@link #createProducerMessageHandler(String, ProducerProperties)} </li>
 * <li>{@link #createConsumerDestinationIfNecessary(String, String, ConsumerProperties)} </li>
 * <li>{@link #createConsumerEndpoint(String, String, Object, ConsumerProperties)}</li>
 * </ul>
 * @author Marius Bogoevici
 * @since 1.1
 */
public abstract class AbstractMessageChannelBinder<C extends ConsumerProperties, P extends ProducerProperties, D>
		extends AbstractBinder<MessageChannel, C, P> {

	protected static final ExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new
			EmbeddedHeadersMessageConverter();

	/**
	 * Indicates whether the implementation and the message broker have
	 * native support for message headers. If false, headers will be
	 * embedded in the message payloads.
	 */
	private final boolean supportsHeadersNatively;

	/**
	 * Indicates what headers are to be embedded in the payload if
	 * {@link #supportsHeadersNatively} is true.
	 */
	private final String[] headersToEmbed;

	public AbstractMessageChannelBinder(boolean supportsHeadersNatively, String[] headersToEmbed) {
		this.supportsHeadersNatively = supportsHeadersNatively;
		this.headersToEmbed = headersToEmbed;
	}

	/**
	 * Binds an outbound channel to a given destination. The implementation delegates to
	 * {@link #createProducerDestinationIfNecessary(String, ProducerProperties)}
	 * and {@link #createProducerMessageHandler(String, ProducerProperties)} for
	 * handling the middleware specific logic. If the returned producer message handler is an
	 * {@link InitializingBean} then {@link InitializingBean#afterPropertiesSet()} will be
	 * called on it. Similarly, if the returned producer message handler endpoint is a
	 * {@link Lifecycle}, then {@link Lifecycle#start()} will be called on it.
	 * @param destination        the name of the destination
	 * @param outputChannel      the channel to be bound
	 * @param producerProperties the {@link ProducerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindProducer(final String destination, MessageChannel outputChannel,
			final P producerProperties) throws BinderException {
		Assert.isInstanceOf(SubscribableChannel.class, outputChannel,
				"Binding is supported only for SubscribableChannel instances");
		createProducerDestinationIfNecessary(destination, producerProperties);
		final MessageHandler producerMessageHandler;
		try {
			producerMessageHandler = createProducerMessageHandler(destination, producerProperties);
			if (producerMessageHandler instanceof InitializingBean) {
				((InitializingBean) producerMessageHandler).afterPropertiesSet();
			}
		}
		catch (Exception e) {
			if (e instanceof BinderException) {
				throw (BinderException) e;
			}
			else {
				throw new BinderException("Exception thrown while building outbound endpoint", e);
			}
		}
		if (producerMessageHandler instanceof Lifecycle) {
			((Lifecycle) producerMessageHandler).start();
		}
		((SubscribableChannel) outputChannel).subscribe(
				new SendingHandler(producerMessageHandler, !this.supportsHeadersNatively && HeaderMode.embeddedHeaders
						.equals(producerProperties.getHeaderMode()), this.headersToEmbed));

		return new DefaultBinding<MessageChannel>(destination, null, outputChannel,
				producerMessageHandler instanceof Lifecycle ? (Lifecycle) producerMessageHandler : null) {

			@Override
			public void afterUnbind() {
				afterUnbindProducer(destination, producerProperties);
			}
		};
	}

	/**
	 * Creates target destinations for outbound channels. The implementation
	 * is middleware-specific.
	 * @param name       the name of the producer destination
	 * @param properties producer properties
	 */
	protected abstract void createProducerDestinationIfNecessary(String name, P properties);

	/**
	 * Creates a {@link MessageHandler} with the ability to send data to the
	 * target middleware. If the returned instance is also a {@link Lifecycle},
	 * it will be stopped automatically by the binder.
	 * <p>
	 * In order to be fully compliant, the {@link MessageHandler} of the binder
	 * must observe the following headers:
	 * <ul>
	 * <li>{@link BinderHeaders#PARTITION_HEADER} - indicates the target
	 * partition where the message must be sent</li>
	 * </ul>
	 * <p>
	 * @param destination        the name of the target destination
	 * @param producerProperties the producer properties
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception
	 */
	protected abstract MessageHandler createProducerMessageHandler(String destination, P producerProperties)
			throws Exception;

	/**
	 * Invoked after the unbinding of a producer. Subclasses may override this to provide
	 * their own logic for dealing with unbinding.
	 * @param destination        the bound destination
	 * @param producerProperties the producer properties
	 */
	protected void afterUnbindProducer(String destination, P producerProperties) {
	}

	/**
	 * Binds an inbound channel to a given destination. The implementation delegates to
	 * {@link #createConsumerDestinationIfNecessary(String, String, ConsumerProperties)}
	 * and {@link #createConsumerEndpoint(String, String, Object, ConsumerProperties)}
	 * for handling middleware-specific logic. If the returned consumer endpoint is an
	 * {@link InitializingBean} then {@link InitializingBean#afterPropertiesSet()} will be
	 * called on it. Similarly, if the returned consumer endpoint is a {@link Lifecycle},
	 * then {@link Lifecycle#start()} will be called on it.
	 * @param name         the name of the destination
	 * @param group        the consumer group
	 * @param inputChannel the channel to be bound
	 * @param properties   the {@link ConsumerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel,
			final C properties) throws BinderException {
		MessageProducer consumerEndpoint = null;
		try {
			D destination = createConsumerDestinationIfNecessary(name, group, properties);
			final boolean extractEmbeddedHeaders = HeaderMode.embeddedHeaders.equals(
					properties.getHeaderMode()) && !this.supportsHeadersNatively;
			ReceivingHandler rh = new ReceivingHandler(extractEmbeddedHeaders);
			rh.setOutputChannel(inputChannel);
			final FixedSubscriberChannel bridge = new FixedSubscriberChannel(rh);
			bridge.setBeanName("bridge." + name);
			consumerEndpoint = createConsumerEndpoint(name, group, destination, properties);
			consumerEndpoint.setOutputChannel(bridge);
			if (consumerEndpoint instanceof InitializingBean) {
				((InitializingBean) consumerEndpoint).afterPropertiesSet();
			}
			if (consumerEndpoint instanceof Lifecycle) {
				((Lifecycle) consumerEndpoint).start();
			}
			final Object endpoint = consumerEndpoint;
			EventDrivenConsumer edc = new EventDrivenConsumer(bridge, rh);
			edc.setBeanName("inbound." + groupedName(name, group));
			edc.start();
			return new DefaultBinding<MessageChannel>(name, group, inputChannel,
					endpoint instanceof Lifecycle ? (Lifecycle) endpoint : null) {

				@Override
				protected void afterUnbind() {
					AbstractMessageChannelBinder.this.afterUnbindConsumer(this.name, this.group, properties);
				}
			};
		}
		catch (Exception e) {
			if (consumerEndpoint instanceof Lifecycle) {
				((Lifecycle) consumerEndpoint).stop();
			}
			if (e instanceof BinderException) {
				throw (BinderException) e;
			}
			else {
				throw new BinderException("Exception thrown while starting consumer: ", e);
			}
		}
	}

	/**
	 * Creates the middleware destination the consumer will start to consume data from.
	 * @param name       the name of the destination
	 * @param group      the consumer group
	 * @param properties consumer properties
	 * @return reference to the consumer destination
	 */
	protected abstract D createConsumerDestinationIfNecessary(String name, String group, C properties);

	/**
	 * Creates {@link MessageProducer} that receives data from the consumer destination.
	 * will be started and stopped by the binder.
	 * @param name        the name of the target destination
	 * @param group       the consumer group
	 * @param destination reference to the consumer destination
	 * @param properties  the consumer properties
	 * @return the consumer endpoint.
	 */
	protected abstract MessageProducer createConsumerEndpoint(String name, String group, D destination,
			C properties);

	/**
	 * Invoked after the unbinding of a consumer. The binder implementation can override
	 * this method to provide their own logic (e.g. for cleaning up destinations).
	 * @param destination        the consumer destination
	 * @param group              the consumer group
	 * @param consumerProperties the consumer properties
	 */
	protected void afterUnbindConsumer(String destination, String group, C consumerProperties) {
	}

	private final class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		private final boolean extractEmbeddedHeaders;

		private ReceivingHandler(boolean extractEmbeddedHeaders) {
			this.extractEmbeddedHeaders = extractEmbeddedHeaders;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Object handleRequestMessage(Message<?> requestMessage) {
			MessageValues messageValues;
			if (this.extractEmbeddedHeaders) {
				try {
					messageValues = AbstractMessageChannelBinder.this.embeddedHeadersMessageConverter.extractHeaders(
							(Message<byte[]>) requestMessage, true);
				}
				catch (Exception e) {
					AbstractMessageChannelBinder.this.logger.error(
							EmbeddedHeadersMessageConverter.decodeExceptionMessage(
									requestMessage), e);
					messageValues = new MessageValues(requestMessage);
				}
				messageValues = deserializePayloadIfNecessary(messageValues);
			}
			else {
				messageValues = deserializePayloadIfNecessary(requestMessage);
			}
			return messageValues.toMessage();
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent the message from being copied again in superclass
			return false;
		}
	}

	private final class SendingHandler extends AbstractMessageHandler implements Lifecycle {

		private final boolean embedHeaders;

		private final String[] embeddedHeaders;

		private final MessageHandler delegate;

		private SendingHandler(MessageHandler delegate, boolean embedHeaders,
				String[] headersToEmbed) {
			this.delegate = delegate;
			this.setBeanFactory(AbstractMessageChannelBinder.this.getBeanFactory());
			this.embedHeaders = embedHeaders;
			this.embeddedHeaders = headersToEmbed;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues transformed = serializePayloadIfNecessary(message);
			byte[] payload;
			if (this.embedHeaders) {
				Object contentType = transformed.get(MessageHeaders.CONTENT_TYPE);
				// transform content type headers to String, so that they can be properly embedded in JSON
				if (contentType instanceof MimeType) {
					transformed.put(MessageHeaders.CONTENT_TYPE, contentType.toString());
				}
				Object originalContentType = transformed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
				if (originalContentType instanceof MimeType) {
					transformed.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
				}
				payload = AbstractMessageChannelBinder.this.embeddedHeadersMessageConverter.embedHeaders(transformed,
						this.embeddedHeaders);
			}
			else {
				payload = (byte[]) transformed.getPayload();
			}
			if (!this.embedHeaders && !AbstractMessageChannelBinder.this.supportsHeadersNatively) {
				Object contentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
				if (contentType != null && !contentType.toString().equals(MediaType.APPLICATION_OCTET_STREAM_VALUE)) {
					this.logger.error(
							"Raw mode supports only " + MediaType.APPLICATION_OCTET_STREAM_VALUE + " content type"
									+ message.getPayload().getClass());
				}
				if (message.getPayload() instanceof byte[]) {
					payload = (byte[]) message.getPayload();
				}
				else {
					throw new BinderException("Raw mode supports only byte[] payloads but value sent was of type "
							+ message.getPayload().getClass());
				}
			}
			this.delegate.handleMessage(getMessageBuilderFactory().withPayload(payload)
					.copyHeaders(transformed.getHeaders())
					.build());
		}

		@Override
		public void start() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).start();
			}
		}

		@Override
		public void stop() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).stop();
			}
		}

		@Override
		public boolean isRunning() {
			return this.delegate instanceof Lifecycle && ((Lifecycle) this.delegate).isRunning();
		}
	}
}
