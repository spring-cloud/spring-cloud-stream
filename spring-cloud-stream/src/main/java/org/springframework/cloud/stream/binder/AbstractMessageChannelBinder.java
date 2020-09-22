/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.logging.Log;

import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.cloud.stream.config.ConsumerEndpointCustomizer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.retry.RecoveryCallback;
import org.springframework.util.Assert;

/**
 * {@link AbstractBinder} that serves as base class for {@link MessageChannel} binders.
 * Implementors must implement the following methods:
 * <ul>
 * <li>{@link #createProducerMessageHandler(ProducerDestination, ProducerProperties, MessageChannel)}</li>
 * <li>{@link #createConsumerEndpoint(ConsumerDestination, String, ConsumerProperties)}
 * </li>
 * </ul>
 *
 * @param <C> the consumer properties type
 * @param <P> the producer properties type
 * @param <PP> the provisioning producer properties type
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.1
 */
public abstract class AbstractMessageChannelBinder<C extends ConsumerProperties, P extends ProducerProperties, PP extends ProvisioningProvider<C, P>>
		extends AbstractBinder<MessageChannel, C, P> implements
		PollableConsumerBinder<MessageHandler, C>, ApplicationEventPublisherAware {


	/**
	 * {@link ProvisioningProvider} delegated by the downstream binder implementations.
	 */
	protected final PP provisioningProvider;

	private final EmbeddedHeadersChannelInterceptor embeddedHeadersChannelInterceptor = new EmbeddedHeadersChannelInterceptor(
			this.logger);

	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Indicates which headers are to be embedded in the payload if a binding requires
	 * embedding headers.
	 */
	private final String[] headersToEmbed;

	private final ListenerContainerCustomizer<?> containerCustomizer;

	private final MessageSourceCustomizer<?> sourceCustomizer;

	private ProducerMessageHandlerCustomizer<MessageHandler> handlerCustomizer =
		(handler, destination) -> { };

	private ConsumerEndpointCustomizer<MessageProducer> consumerCustomizer =
		(adapter, destination, group) -> { };

	private ApplicationEventPublisher applicationEventPublisher;

	public AbstractMessageChannelBinder(String[] headersToEmbed,
			PP provisioningProvider) {
		this(headersToEmbed, provisioningProvider, null, null);
	}

	public AbstractMessageChannelBinder(String[] headersToEmbed, PP provisioningProvider,
			@Nullable ListenerContainerCustomizer<?> containerCustomizer,
			@Nullable MessageSourceCustomizer<?> sourceCustomizer) {

		SimpleModule module = new SimpleModule();
		module.addSerializer(Expression.class, new ExpressionSerializer(Expression.class));
		objectMapper.registerModule(module);

		this.headersToEmbed = headersToEmbed == null ? new String[0] : headersToEmbed;
		this.provisioningProvider = provisioningProvider;
		this.containerCustomizer = containerCustomizer == null ? (c, q, g) -> {
		} : containerCustomizer;
		this.sourceCustomizer = sourceCustomizer == null ? (s, q, g) -> {
		} : sourceCustomizer;
	}

	protected ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Configure an optional {@link ProducerMessageHandlerCustomizer} for further
	 * configuration of producer {@link MessageHandler} instances created by the binder.
	 * @param handlerCustomizer the {@link ProducerMessageHandlerCustomizer} to use.
	 * @since 3.0
	 */
	@SuppressWarnings("unchecked")
	public void setProducerMessageHandlerCustomizer(
		@Nullable ProducerMessageHandlerCustomizer<? extends MessageHandler> handlerCustomizer) {

		this.handlerCustomizer =
			handlerCustomizer == null
				? (handler, destination) -> { }
				: (ProducerMessageHandlerCustomizer<MessageHandler>) handlerCustomizer;
	}

	/**
	 * Configure an optional {@link ConsumerEndpointCustomizer} for further
	 * configuration of consumer {@link MessageProducer} instances created by the binder.
	 * @param endpointCustomizer the {@link ConsumerEndpointCustomizer} to use.
	 * @since 3.0
	 */
	@SuppressWarnings("unchecked")
	public void setConsumerEndpointCustomizer(
		@Nullable ConsumerEndpointCustomizer<? extends MessageProducer> endpointCustomizer) {

		this.consumerCustomizer =
				endpointCustomizer == null
				? (handler, destination, group) -> { }
				: (ConsumerEndpointCustomizer<MessageProducer>) endpointCustomizer;
	}

	@SuppressWarnings("unchecked")
	protected <L> ListenerContainerCustomizer<L> getContainerCustomizer() {
		return (ListenerContainerCustomizer<L>) this.containerCustomizer;
	}

	@SuppressWarnings("unchecked")
	protected <S> MessageSourceCustomizer<S> getMessageSourceCustomizer() {
		return (MessageSourceCustomizer<S>) this.sourceCustomizer;
	}

	/**
	 * Binds an outbound channel to a given destination. The implementation delegates to
	 * {@link ProvisioningProvider#provisionProducerDestination(String, ProducerProperties)}
	 * and
	 * {@link #createProducerMessageHandler(ProducerDestination, ProducerProperties, MessageChannel)}
	 * for handling the middleware specific logic. If the returned producer message
	 * handler is an {@link InitializingBean} then
	 * {@link InitializingBean#afterPropertiesSet()} will be called on it. Similarly, if
	 * the returned producer message handler endpoint is a {@link Lifecycle}, then
	 * {@link Lifecycle#start()} will be called on it.
	 * @param destination the name of the destination
	 * @param outputChannel the channel to be bound
	 * @param producerProperties the {@link ProducerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindProducer(final String destination,
			MessageChannel outputChannel, final P producerProperties)
			throws BinderException {
		Assert.isInstanceOf(SubscribableChannel.class, outputChannel,
				"Binding is supported only for SubscribableChannel instances");
		final MessageHandler producerMessageHandler;
		final ProducerDestination producerDestination;
		try {
			producerDestination = this.provisioningProvider
					.provisionProducerDestination(destination, producerProperties);
			SubscribableChannel errorChannel = producerProperties.isErrorChannelEnabled()
					? registerErrorInfrastructure(producerDestination) : null;
			producerMessageHandler = createProducerMessageHandler(producerDestination,
					producerProperties, outputChannel, errorChannel);
			customizeProducerMessageHandler(producerMessageHandler, producerDestination.getName());
			if (producerMessageHandler instanceof InitializingBean) {
				((InitializingBean) producerMessageHandler).afterPropertiesSet();
			}
		}
		catch (Exception e) {
			if (e instanceof BinderException) {
				throw (BinderException) e;
			}
			else if (e instanceof ProvisioningException) {
				throw (ProvisioningException) e;
			}
			else {
				throw new BinderException(
						"Exception thrown while building outbound endpoint", e);
			}
		}

		if (producerProperties.isAutoStartup()
				&& producerMessageHandler instanceof Lifecycle) {
			((Lifecycle) producerMessageHandler).start();
		}
		this.postProcessOutputChannel(outputChannel, producerProperties);

		((SubscribableChannel) outputChannel)
				.subscribe(new SendingHandler(producerMessageHandler,
						HeaderMode.embeddedHeaders
								.equals(producerProperties.getHeaderMode()),
						this.headersToEmbed, useNativeEncoding(producerProperties)));

		Binding<MessageChannel> binding = new DefaultBinding<MessageChannel>(destination,
				outputChannel, producerMessageHandler instanceof Lifecycle
						? (Lifecycle) producerMessageHandler : null) {

			@Override
			public Map<String, Object> getExtendedInfo() {
				return doGetExtendedInfo(destination, producerProperties);
			}

			@Override
			public boolean isInput() {
				return false;
			}

			@Override
			public void afterUnbind() {
				try {
					destroyErrorInfrastructure(producerDestination);
					if (producerMessageHandler instanceof DisposableBean) {
						((DisposableBean) producerMessageHandler).destroy();
					}
				}
				catch (Exception e) {
					AbstractMessageChannelBinder.this.logger
							.error("Exception thrown while unbinding " + toString(), e);
				}
				afterUnbindProducer(producerDestination, producerProperties);
			}
		};

		doPublishEvent(new BindingCreatedEvent(binding));
		return binding;
	}

	private void customizeProducerMessageHandler(MessageHandler producerMessageHandler, String destinationName) {
		this.handlerCustomizer.configure(producerMessageHandler, destinationName);
	}

	/**
	 * Whether the producer for the destination being created should be configured to use
	 * native encoding which may, or may not, be determined from the properties. For
	 * example, a transactional kafka binder uses a common producer for all destinations.
	 * The default implementation returns {@link ProducerProperties#isUseNativeEncoding()}.
	 * @param producerProperties the properties.
	 * @return true to use native encoding.
	 */
	protected boolean useNativeEncoding(P producerProperties) {
		return producerProperties.isUseNativeEncoding();
	}

	/**
	 * Allows subclasses to perform post processing on the channel - for example to add
	 * more interceptors.
	 * @param outputChannel the channel.
	 * @param producerProperties the producer properties.
	 */
	protected void postProcessOutputChannel(MessageChannel outputChannel,
			P producerProperties) {
		// default no-op
	}

	/**
	 * Create a {@link MessageHandler} with the ability to send data to the target
	 * middleware. If the returned instance is also a {@link Lifecycle}, it will be
	 * stopped automatically by the binder.
	 * <p>
	 * In order to be fully compliant, the {@link MessageHandler} of the binder must
	 * observe the following headers:
	 * <ul>
	 * <li>{@link BinderHeaders#PARTITION_HEADER} - indicates the target partition where
	 * the message must be sent</li>
	 * </ul>
	 * <p>
	 * @param destination the name of the target destination.
	 * @param producerProperties the producer properties.
	 * @param channel the channel to bind.
	 * @param errorChannel the error channel (if enabled, otherwise null). If not null,
	 * the binder must wire this channel into the producer endpoint so that errors are
	 * forwarded to it.
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception when producer messsage handler failed to be created
	 */
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			P producerProperties, MessageChannel channel, MessageChannel errorChannel)
			throws Exception {
		return createProducerMessageHandler(destination, producerProperties,
				errorChannel);
	}

	/**
	 * Create a {@link MessageHandler} with the ability to send data to the target
	 * middleware. If the returned instance is also a {@link Lifecycle}, it will be
	 * stopped automatically by the binder.
	 * <p>
	 * In order to be fully compliant, the {@link MessageHandler} of the binder must
	 * observe the following headers:
	 * <ul>
	 * <li>{@link BinderHeaders#PARTITION_HEADER} - indicates the target partition where
	 * the message must be sent</li>
	 * </ul>
	 * <p>
	 * @param destination the name of the target destination
	 * @param producerProperties the producer properties
	 * @param errorChannel the error channel (if enabled, otherwise null). If not null,
	 * the binder must wire this channel into the producer endpoint so that errors are
	 * forwarded to it.
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception upon failure to create the producer message handler
	 */
	protected abstract MessageHandler createProducerMessageHandler(
			ProducerDestination destination, P producerProperties,
			MessageChannel errorChannel) throws Exception;

	/**
	 * Invoked after the unbinding of a producer. Subclasses may override this to provide
	 * their own logic for dealing with unbinding.
	 * @param destination the bound destination
	 * @param producerProperties the producer properties
	 */
	protected void afterUnbindProducer(ProducerDestination destination,
			P producerProperties) {
	}

	/**
	 * Binds an inbound channel to a given destination. The implementation delegates to
	 * {@link ProvisioningProvider#provisionConsumerDestination(String, String, ConsumerProperties)}
	 * and
	 * {@link #createConsumerEndpoint(ConsumerDestination, String, ConsumerProperties)}
	 * for handling middleware-specific logic. If the returned consumer endpoint is an
	 * {@link InitializingBean} then {@link InitializingBean#afterPropertiesSet()} will be
	 * called on it. Similarly, if the returned consumer endpoint is a {@link Lifecycle},
	 * then {@link Lifecycle#start()} will be called on it.
	 * @param name the name of the destination
	 * @param group the consumer group
	 * @param inputChannel the channel to be bound
	 * @param properties the {@link ConsumerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindConsumer(String name, String group,
			MessageChannel inputChannel, final C properties) throws BinderException {
		MessageProducer consumerEndpoint = null;
		try {
			ConsumerDestination destination = this.provisioningProvider
					.provisionConsumerDestination(name, group, properties);

			if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {
				enhanceMessageChannel(inputChannel);
			}
			consumerEndpoint = createConsumerEndpoint(destination, group, properties);
			consumerEndpoint.setOutputChannel(inputChannel);
			this.consumerCustomizer.configure(consumerEndpoint, name, group);
			if (consumerEndpoint instanceof InitializingBean) {
				((InitializingBean) consumerEndpoint).afterPropertiesSet();
			}
			if (properties.isAutoStartup() && consumerEndpoint instanceof Lifecycle) {
				((Lifecycle) consumerEndpoint).start();
			}

			Binding<MessageChannel> binding = new DefaultBinding<MessageChannel>(name,
					group, inputChannel, consumerEndpoint instanceof Lifecycle
							? (Lifecycle) consumerEndpoint : null) {

				@Override
				public Map<String, Object> getExtendedInfo() {
					return doGetExtendedInfo(destination, properties);
				}

				@Override
				public boolean isInput() {
					return true;
				}

				@Override
				protected void afterUnbind() {
					try {
						if (getEndpoint() instanceof DisposableBean) {
							((DisposableBean) getEndpoint()).destroy();
						}
					}
					catch (Exception e) {
						AbstractMessageChannelBinder.this.logger.error(
								"Exception thrown while unbinding " + toString(), e);
					}
					afterUnbindConsumer(destination, this.group, properties);
					destroyErrorInfrastructure(destination, this.group, properties);
				}

			};
			doPublishEvent(new BindingCreatedEvent(binding));
			return binding;
		}
		catch (Exception e) {
			if (consumerEndpoint instanceof Lifecycle) {
				((Lifecycle) consumerEndpoint).stop();
			}
			if (e instanceof BinderException) {
				throw (BinderException) e;
			}
			else if (e instanceof ProvisioningException) {
				throw (ProvisioningException) e;
			}
			else {
				throw new BinderException("Exception thrown while starting consumer: ", e);
			}
		}
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name,
			String group, final PollableSource<MessageHandler> inboundBindTarget,
			C properties) {
		Assert.isInstanceOf(DefaultPollableMessageSource.class, inboundBindTarget);
		DefaultPollableMessageSource bindingTarget = (DefaultPollableMessageSource) inboundBindTarget;
		ConsumerDestination destination = this.provisioningProvider
				.provisionConsumerDestination(name, group, properties);
		if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {
			bindingTarget.addInterceptor(0, this.embeddedHeadersChannelInterceptor);
		}
		final PolledConsumerResources resources = createPolledConsumerResources(name,
				group, destination, properties);

		MessageSource<?> messageSource = resources.getSource();
		if (messageSource instanceof BeanFactoryAware) {
			((BeanFactoryAware) messageSource).setBeanFactory(getApplicationContext().getBeanFactory());
		}
		bindingTarget.setSource(messageSource);
		if (resources.getErrorInfrastructure() != null) {
			if (resources.getErrorInfrastructure().getErrorChannel() != null) {
				bindingTarget.setErrorChannel(
						resources.getErrorInfrastructure().getErrorChannel());
			}
			ErrorMessageStrategy ems = getErrorMessageStrategy();
			if (ems != null) {
				bindingTarget.setErrorMessageStrategy(ems);
			}
		}
		if (properties.getMaxAttempts() > 1) {
			bindingTarget.setRetryTemplate(buildRetryTemplate(properties));
			bindingTarget.setRecoveryCallback(getPolledConsumerRecoveryCallback(
					resources.getErrorInfrastructure(), properties));
		}
		postProcessPollableSource(bindingTarget);
		if (properties.isAutoStartup() && resources.getSource() instanceof Lifecycle) {
			((Lifecycle) resources.getSource()).start();
		}
		Binding<PollableSource<MessageHandler>> binding = new DefaultBinding<PollableSource<MessageHandler>>(
				name, group, inboundBindTarget, resources.getSource() instanceof Lifecycle
						? (Lifecycle) resources.getSource() : null) {

			@Override
			public Map<String, Object> getExtendedInfo() {
				return doGetExtendedInfo(destination, properties);
			}

			@Override
			public boolean isInput() {
				return true;
			}

			@Override
			public void afterUnbind() {
				afterUnbindConsumer(destination, this.group, properties);
				destroyErrorInfrastructure(destination, this.group, properties);
			}

		};

		doPublishEvent(new BindingCreatedEvent(binding));
		return binding;
	}

	protected void postProcessPollableSource(DefaultPollableMessageSource bindingTarget) {
	}

	/**
	 * Implementations can override the default {@link ErrorMessageSendingRecoverer}.
	 * @param errorInfrastructure the infrastructure.
	 * @param properties the consumer properties.
	 * @return the recoverer.
	 */
	protected RecoveryCallback<Object> getPolledConsumerRecoveryCallback(
			ErrorInfrastructure errorInfrastructure, C properties) {
		return errorInfrastructure.getRecoverer();
	}

	protected PolledConsumerResources createPolledConsumerResources(String name,
			String group, ConsumerDestination destination, C consumerProperties) {
		throw new UnsupportedOperationException(
				"This binder does not support pollable consumers");
	}

	private void enhanceMessageChannel(MessageChannel inputChannel) {
		((AbstractMessageChannel) inputChannel).addInterceptor(0,
				this.embeddedHeadersChannelInterceptor);
	}

	/**
	 * Creates {@link MessageProducer} that receives data from the consumer destination.
	 * will be started and stopped by the binder.
	 * @param group the consumer group
	 * @param destination reference to the consumer destination
	 * @param properties the consumer properties
	 * @return the consumer endpoint.
	 * @throws Exception when consumer endpoint creation failed.
	 */
	protected abstract MessageProducer createConsumerEndpoint(
			ConsumerDestination destination, String group, C properties) throws Exception;

	/**
	 * Invoked after the unbinding of a consumer. The binder implementation can override
	 * this method to provide their own logic (e.g. for cleaning up destinations).
	 * @param destination the consumer destination
	 * @param group the consumer group
	 * @param consumerProperties the consumer properties
	 */
	protected void afterUnbindConsumer(ConsumerDestination destination, String group,
			C consumerProperties) {
	}

	/**
	 * Register an error channel for the destination when an async send error is received.
	 * Bridge the channel to the global error channel (if present).
	 * @param destination the destination.
	 * @return the channel.
	 */
	private SubscribableChannel registerErrorInfrastructure(
			ProducerDestination destination) {

		String errorChannelName = errorsBaseName(destination);
		SubscribableChannel errorChannel;
		if (getApplicationContext().containsBean(errorChannelName)) {
			Object errorChannelObject = getApplicationContext().getBean(errorChannelName);
			if (!(errorChannelObject instanceof SubscribableChannel)) {
				throw new IllegalStateException("Error channel '" + errorChannelName
						+ "' must be a SubscribableChannel");
			}
			errorChannel = (SubscribableChannel) errorChannelObject;
		}
		else {
			errorChannel = new PublishSubscribeChannel();
			((GenericApplicationContext) getApplicationContext()).registerBean(
					errorChannelName, SubscribableChannel.class, () -> errorChannel);
		}
		MessageChannel defaultErrorChannel = null;
		if (getApplicationContext()
				.containsBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)) {
			defaultErrorChannel = getApplicationContext().getBean(
					IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
					MessageChannel.class);
		}
		if (defaultErrorChannel != null) {
			BridgeHandler errorBridge = new BridgeHandler();
			errorBridge.setOutputChannel(defaultErrorChannel);
			errorChannel.subscribe(errorBridge);
			String errorBridgeHandlerName = getErrorBridgeName(destination);
			((GenericApplicationContext) getApplicationContext()).registerBean(
					errorBridgeHandlerName, BridgeHandler.class, () -> errorBridge);
		}
		return errorChannel;
	}

	/**
	 * Build an errorChannelRecoverer that writes to a pub/sub channel for the destination
	 * when an exception is thrown to a consumer.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @return the ErrorInfrastructure which is a holder for the error channel, the
	 * recoverer and the message handler that is subscribed to the channel.
	 */
	protected final ErrorInfrastructure registerErrorInfrastructure(
			ConsumerDestination destination, String group, C consumerProperties) {

		return registerErrorInfrastructure(destination, group, consumerProperties, false);
	}

	/**
	 * Build an errorChannelRecoverer that writes to a pub/sub channel for the destination
	 * when an exception is thrown to a consumer.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @param polled true if this is for a polled consumer.
	 * @return the ErrorInfrastructure which is a holder for the error channel, the
	 * recoverer and the message handler that is subscribed to the channel.
	 */
	protected final ErrorInfrastructure registerErrorInfrastructure(
			ConsumerDestination destination, String group, C consumerProperties,
			boolean polled) {

		ErrorMessageStrategy errorMessageStrategy = getErrorMessageStrategy();

		String errorChannelName = errorsBaseName(destination, group, consumerProperties);
		SubscribableChannel errorChannel;
		if (getApplicationContext().containsBean(errorChannelName)) {
			Object errorChannelObject = getApplicationContext().getBean(errorChannelName);

			Assert.isInstanceOf(SubscribableChannel.class, errorChannelObject,
					"Error channel '" + errorChannelName
							+ "' must be a SubscribableChannel");
			errorChannel = (SubscribableChannel) errorChannelObject;
		}
		else {
			BinderErrorChannel binderErrorChannel = new BinderErrorChannel();
			binderErrorChannel.setComponentName(errorChannelName);
			errorChannel = binderErrorChannel;

			((GenericApplicationContext) getApplicationContext()).registerBean(
					errorChannelName, SubscribableChannel.class, () -> errorChannel);
		}
		ErrorMessageSendingRecoverer recoverer;
		if (errorMessageStrategy == null) {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel);
		}
		else {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel,
					errorMessageStrategy);
		}

		String recovererBeanName = getErrorRecovererName(destination, group,
				consumerProperties);
		((GenericApplicationContext) getApplicationContext()).registerBean(
				recovererBeanName, ErrorMessageSendingRecoverer.class, () -> recoverer);
		MessageHandler handler;
		if (polled) {
			handler = getPolledConsumerErrorMessageHandler(destination, group,
					consumerProperties);
		}
		else {
			handler = getErrorMessageHandler(destination, group, consumerProperties);
		}
		MessageChannel defaultErrorChannel = null;
		if (getApplicationContext()
				.containsBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)) {
			defaultErrorChannel = getApplicationContext().getBean(
					IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
					MessageChannel.class);
		}
		if (handler == null && errorChannel instanceof LastSubscriberAwareChannel) {
			handler = getDefaultErrorMessageHandler(
					(LastSubscriberAwareChannel) errorChannel,
					defaultErrorChannel != null);
		}
		String errorMessageHandlerName = getErrorMessageHandlerName(destination, group,
				consumerProperties);

		if (handler != null) {
			if (this.isSubscribable(errorChannel)) {
				MessageHandler errorHandler = handler;
				((GenericApplicationContext) getApplicationContext()).registerBean(
						errorMessageHandlerName, MessageHandler.class,
						() -> errorHandler);
				errorChannel.subscribe(handler);
			}
			else {
				this.logger.warn("The provided errorChannel '" + errorChannelName
						+ "' is an instance of DirectChannel, "
						+ "so no more subscribers could be added which may affect DLQ processing. "
						+ "Resolution: Configure your own errorChannel as "
						+ "an instance of PublishSubscribeChannel");
			}
		}

		if (defaultErrorChannel != null) {
			if (this.isSubscribable(errorChannel)) {
				BridgeHandler errorBridge = new BridgeHandler();
				errorBridge.setOutputChannel(defaultErrorChannel);
				errorChannel.subscribe(errorBridge);

				String errorBridgeHandlerName = getErrorBridgeName(destination, group,
						consumerProperties);
				((GenericApplicationContext) getApplicationContext()).registerBean(
						errorBridgeHandlerName, BridgeHandler.class, () -> errorBridge);
			}
			else {
				this.logger.warn("The provided errorChannel '" + errorChannelName
						+ "' is an instance of DirectChannel, "
						+ "so no more subscribers could be added and no error messages will be sent to global error channel. "
						+ "Resolution: Configure your own errorChannel as "
						+ "an instance of PublishSubscribeChannel");
			}
		}
		return new ErrorInfrastructure(errorChannel, recoverer, handler);
	}

	private boolean isSubscribable(SubscribableChannel errorChannel) {
		if (errorChannel instanceof PublishSubscribeChannel) {
			return true;
		}
		return errorChannel instanceof AbstractSubscribableChannel
				? ((AbstractSubscribableChannel) errorChannel).getSubscriberCount() == 0
				: true;
	}

	private void destroyErrorInfrastructure(ProducerDestination destination) {
		String errorChannelName = errorsBaseName(destination);
		String errorBridgeHandlerName = getErrorBridgeName(destination);
		MessageHandler bridgeHandler = null;
		if (getApplicationContext().containsBean(errorBridgeHandlerName)) {
			bridgeHandler = getApplicationContext().getBean(errorBridgeHandlerName,
					MessageHandler.class);
		}
		if (getApplicationContext().containsBean(errorChannelName)) {
			SubscribableChannel channel = getApplicationContext()
					.getBean(errorChannelName, SubscribableChannel.class);
			if (bridgeHandler != null) {
				channel.unsubscribe(bridgeHandler);
				((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
						.destroySingleton(errorBridgeHandlerName);
			}
			((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
					.destroySingleton(errorChannelName);
		}
	}

	private void destroyErrorInfrastructure(ConsumerDestination destination, String group,
			C properties) {
		try {
			String recoverer = getErrorRecovererName(destination, group, properties);

			destroyBean(recoverer);

			String errorChannelName = errorsBaseName(destination, group, properties);
			String errorMessageHandlerName = getErrorMessageHandlerName(destination,
					group, properties);
			String errorBridgeHandlerName = getErrorBridgeName(destination, group,
					properties);
			MessageHandler bridgeHandler = null;
			if (getApplicationContext().containsBean(errorBridgeHandlerName)) {
				bridgeHandler = getApplicationContext().getBean(errorBridgeHandlerName,
						MessageHandler.class);
			}
			MessageHandler handler = null;
			if (getApplicationContext().containsBean(errorMessageHandlerName)) {
				handler = getApplicationContext().getBean(errorMessageHandlerName,
						MessageHandler.class);
			}
			if (getApplicationContext().containsBean(errorChannelName)) {
				SubscribableChannel channel = getApplicationContext()
						.getBean(errorChannelName, SubscribableChannel.class);
				if (bridgeHandler != null) {
					channel.unsubscribe(bridgeHandler);
					destroyBean(errorBridgeHandlerName);
				}
				if (handler != null) {
					channel.unsubscribe(handler);
					destroyBean(errorMessageHandlerName);
				}
				destroyBean(errorChannelName);
			}
		}
		catch (IllegalStateException e) {
			// context is shutting down.
		}
	}

	private void destroyBean(String beanName) {
		if (getApplicationContext().containsBeanDefinition(beanName)) {
			((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
					.destroySingleton(beanName);
			((GenericApplicationContext) getApplicationContext())
					.removeBeanDefinition(beanName);
		}
	}

	/**
	 * Binders can return a message handler to be subscribed to the error channel.
	 * Examples might be if the user wishes to (re)publish messages to a DLQ.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @return the handler (may be null, which is the default, causing the exception to be
	 * rethrown).
	 */
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination,
			String group, C consumerProperties) {
		return null;
	}

	/**
	 * Binders can return a message handler to be subscribed to the error channel.
	 * Examples might be if the user wishes to (re)publish messages to a DLQ.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @return the handler (may be null, which is the default, causing the exception to be
	 * rethrown).
	 */
	protected MessageHandler getPolledConsumerErrorMessageHandler(
			ConsumerDestination destination, String group, C consumerProperties) {
		return null;
	}

	/**
	 * Return the default error message handler, which throws the error message payload to
	 * the caller if there are no user handlers subscribed. The handler is ordered so it
	 * runs after any user-defined handlers that are subscribed.
	 * @param errorChannel the error channel.
	 * @param defaultErrorChannelPresent true if the context has a default 'errorChannel'.
	 * @return the handler.
	 */
	protected MessageHandler getDefaultErrorMessageHandler(
			LastSubscriberAwareChannel errorChannel, boolean defaultErrorChannelPresent) {
		return new FinalRethrowingErrorMessageHandler(errorChannel,
				defaultErrorChannelPresent);
	}

	/**
	 * Binders can return an {@link ErrorMessageStrategy} for building error messages;
	 * binder implementations typically might add extra headers to the error message.
	 * @return the implementation - may be null.
	 */
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return null;
	}

	protected String getErrorRecovererName(ConsumerDestination destination, String group,
			C consumerProperties) {
		return errorsBaseName(destination, group, consumerProperties) + ".recoverer";
	}

	protected String getErrorMessageHandlerName(ConsumerDestination destination,
			String group, C consumerProperties) {
		return errorsBaseName(destination, group, consumerProperties) + ".handler";
	}

	protected String getErrorBridgeName(ConsumerDestination destination, String group,
			C consumerProperties) {
		return errorsBaseName(destination, group, consumerProperties) + ".bridge";
	}

	protected String errorsBaseName(ConsumerDestination destination, String group,
			C consumerProperties) {
		return destination.getName() + "." + group + ".errors";
	}

	protected String getErrorBridgeName(ProducerDestination destination) {
		return errorsBaseName(destination) + ".bridge";
	}

	protected String errorsBaseName(ProducerDestination destination) {
		return destination.getName() + ".errors";
	}

	private Map<String, Object> doGetExtendedInfo(Object destination, Object properties) {
		Map<String, Object> extendedInfo = new LinkedHashMap<>();
		extendedInfo.put("bindingDestination", destination.toString());
		extendedInfo.put(properties.getClass().getSimpleName(),
				this.objectMapper.convertValue(properties, Map.class));
		return extendedInfo;
	}

	private void doPublishEvent(ApplicationEvent event) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
	}

	protected static class ErrorInfrastructure {

		private final SubscribableChannel errorChannel;

		private final ErrorMessageSendingRecoverer recoverer;

		private final MessageHandler handler;

		ErrorInfrastructure(SubscribableChannel errorChannel,
				ErrorMessageSendingRecoverer recoverer, MessageHandler handler) {
			this.errorChannel = errorChannel;
			this.recoverer = recoverer;
			this.handler = handler;
		}

		public SubscribableChannel getErrorChannel() {
			return this.errorChannel;
		}

		public ErrorMessageSendingRecoverer getRecoverer() {
			return this.recoverer;
		}

		public MessageHandler getHandler() {
			return this.handler;
		}

	}

	private static final class EmbeddedHeadersChannelInterceptor
			implements ChannelInterceptor {

		protected final Log logger;

		EmbeddedHeadersChannelInterceptor(Log logger) {
			this.logger = logger;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			if (message.getPayload() instanceof byte[]
					&& !message.getHeaders()
							.containsKey(BinderHeaders.NATIVE_HEADERS_PRESENT)
					&& EmbeddedHeaderUtils
							.mayHaveEmbeddedHeaders((byte[]) message.getPayload())) {

				MessageValues messageValues;
				try {
					messageValues = EmbeddedHeaderUtils
							.extractHeaders((Message<byte[]>) message, true);
				}
				catch (Exception e) {
					/*
					 * debug() rather then error() since we don't know for sure that it
					 * really is a message with embedded headers, it just meets the
					 * criteria in EmbeddedHeaderUtils.mayHaveEmbeddedHeaders().
					 */
					if (this.logger.isDebugEnabled()) {
						this.logger.debug(
								EmbeddedHeaderUtils.decodeExceptionMessage(message), e);
					}
					messageValues = new MessageValues(message);
				}
				return messageValues.toMessage();
			}
			return message;
		}

	}

	protected static class PolledConsumerResources {

		private final MessageSource<?> source;

		private final ErrorInfrastructure errorInfrastructure;

		public PolledConsumerResources(MessageSource<?> source,
				ErrorInfrastructure errorInfrastructure) {
			this.source = source;
			this.errorInfrastructure = errorInfrastructure;
		}

		MessageSource<?> getSource() {
			return this.source;
		}

		ErrorInfrastructure getErrorInfrastructure() {
			return this.errorInfrastructure;
		}

	}

	private final class SendingHandler extends AbstractMessageHandler
			implements Lifecycle {

		private final boolean embedHeaders;

		private final String[] embeddedHeaders;

		private final MessageHandler delegate;

		private final boolean useNativeEncoding;

		private SendingHandler(MessageHandler delegate, boolean embedHeaders,
				String[] headersToEmbed, boolean useNativeEncoding) {
			this.delegate = delegate;
			setBeanFactory(AbstractMessageChannelBinder.this.getBeanFactory());
			this.embedHeaders = embedHeaders;
			this.embeddedHeaders = headersToEmbed;
			this.useNativeEncoding = useNativeEncoding;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) {
			Message<?> messageToSend = (this.useNativeEncoding) ? message
					: serializeAndEmbedHeadersIfApplicable(message);
			this.delegate.handleMessage(messageToSend);
		}

		private Message<?> serializeAndEmbedHeadersIfApplicable(Message<?> message) {
			MessageValues transformed = new MessageValues(message);
			Object payload;
			if (this.embedHeaders) {
				Object contentType = transformed.get(MessageHeaders.CONTENT_TYPE);
				// transform content type headers to String, so that they can be properly
				// embedded in JSON
				if (contentType != null) {
					transformed.put(MessageHeaders.CONTENT_TYPE, contentType.toString());
				}
				payload = EmbeddedHeaderUtils.embedHeaders(transformed,
						this.embeddedHeaders);
			}
			else {
				payload = transformed.getPayload();
			}
			return getMessageBuilderFactory().withPayload(payload)
					.copyHeaders(transformed.getHeaders()).build();
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
			return this.delegate instanceof Lifecycle
					&& ((Lifecycle) this.delegate).isRunning();
		}

	}

	@SuppressWarnings("serial")
	private static class ExpressionSerializer extends StdSerializer<Expression> {

		protected ExpressionSerializer(Class<Expression> t) {
			super(t);
		}

		@Override
		public void serialize(Expression value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			gen.writeString(value.getExpressionString());
		}
	}

}
