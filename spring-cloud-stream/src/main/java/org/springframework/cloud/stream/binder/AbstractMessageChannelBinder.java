/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.reactivestreams.Publisher;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.function.IntegrationFlowFunctionSupport;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.MessageChannelReactiveUtils;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.retry.RecoveryCallback;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
		extends AbstractBinder<MessageChannel, C, P>
		implements PollableConsumerBinder<MessageHandler, C>, ApplicationEventPublisherAware {

	private final EmbeddedHeadersChannelInterceptor embeddedHeadersChannelInterceptor =
			new EmbeddedHeadersChannelInterceptor(this.logger);

	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * {@link ProvisioningProvider} delegated by the downstream binder implementations.
	 */
	protected final PP provisioningProvider;

	/**
	 * Indicates which headers are to be embedded in the payload if
	 * a binding requires embedding headers.
	 */
	private final String[] headersToEmbed;

	private final ListenerContainerCustomizer<?> containerCustomizer;

	private ApplicationEventPublisher applicationEventPublisher;

	@Autowired(required = false)
	private Processor processor;

	@Autowired(required = false)
	private IntegrationFlowFunctionSupport integrationFlowFunctionSupport;

	@Autowired(required = false)
	private StreamFunctionProperties streamFunctionProperties;

	public AbstractMessageChannelBinder(String[] headersToEmbed, PP provisioningProvider) {
		this(headersToEmbed, provisioningProvider, null);
	}

	public AbstractMessageChannelBinder(String[] headersToEmbed, PP provisioningProvider, ListenerContainerCustomizer<?> containerCustomizer) {
		this.headersToEmbed = headersToEmbed == null ? new String[0] : headersToEmbed;
		this.provisioningProvider = provisioningProvider;
		this.containerCustomizer = containerCustomizer == null ? (c, q, g) -> { } : containerCustomizer;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	protected ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@SuppressWarnings("unchecked")
	protected <L> ListenerContainerCustomizer<L> getContainerCustomizer() {
		return (ListenerContainerCustomizer<L>) this.containerCustomizer;
	}

	/**
	 * Binds an outbound channel to a given destination. The implementation delegates to
	 * {@link ProvisioningProvider#provisionProducerDestination(String, ProducerProperties)}
	 * and {@link #createProducerMessageHandler(ProducerDestination, ProducerProperties, MessageChannel)}
	 * for handling the middleware specific logic. If the returned producer message
	 * handler is an {@link InitializingBean} then
	 * {@link InitializingBean#afterPropertiesSet()} will be called on it. Similarly, if
	 * the returned producer message handler endpoint is a {@link Lifecycle}, then
	 * {@link Lifecycle#start()} will be called on it.
	 *
	 * @param destination the name of the destination
	 * @param outputChannel the channel to be bound
	 * @param producerProperties the {@link ProducerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindProducer(final String destination, MessageChannel outputChannel,
			final P producerProperties) throws BinderException {
		Assert.isInstanceOf(SubscribableChannel.class, outputChannel,
				"Binding is supported only for SubscribableChannel instances");
		final MessageHandler producerMessageHandler;
		final ProducerDestination producerDestination;
		try {
			producerDestination = this.provisioningProvider.provisionProducerDestination(destination,
					producerProperties);
			SubscribableChannel errorChannel = producerProperties.isErrorChannelEnabled()
					? registerErrorInfrastructure(producerDestination) : null;
			producerMessageHandler = createProducerMessageHandler(producerDestination, producerProperties,
					outputChannel, errorChannel);
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
				throw new BinderException("Exception thrown while building outbound endpoint", e);
			}
		}
		if (producerMessageHandler instanceof Lifecycle) {
			((Lifecycle) producerMessageHandler).start();
		}
		this.postProcessOutputChannel(outputChannel, producerProperties);

		if (this.streamFunctionProperties != null && StringUtils.hasText(this.streamFunctionProperties.getDefinition()) && this.processor == null) {
			outputChannel = this.postProcessOutboundChannelForFunction(outputChannel);
		}

		((SubscribableChannel) outputChannel).subscribe(
				new SendingHandler(producerMessageHandler, HeaderMode.embeddedHeaders
						.equals(producerProperties.getHeaderMode()), this.headersToEmbed,
						useNativeEncoding(producerProperties)));


		Binding<MessageChannel> binding = new DefaultBinding<MessageChannel>(destination, outputChannel,
				producerMessageHandler instanceof Lifecycle ? (Lifecycle) producerMessageHandler : null) {

			@Override
			public Map<String, Object> getExtendedInfo() {
				return doGetExtendedInfo(destination, producerProperties);
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



	/**
	 * Whether the producer for the destination being created should be configured to use
	 * native encoding which may, or may not, be determined from the properties. For
	 * example, a transactional kafka binder uses a common producer for all destinations.
	 * The default implementation returns {@link P#isUseNativeEncoding()}.
	 * @param producerProperties the properties.
	 * @return true to use native encoding.
	 */
	protected boolean useNativeEncoding(P producerProperties) {
		return producerProperties.isUseNativeEncoding();
	}

	/**
	 * Allows subclasses to perform post processing on the channel - for example to
	 * add more interceptors.
	 * @param outputChannel the channel.
	 * @param producerProperties the producer properties.
	 */
	protected void postProcessOutputChannel(MessageChannel outputChannel, P producerProperties) {
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
	 *
	 * @param destination the name of the target destination.
	 * @param producerProperties the producer properties.
	 * @param channel the channel to bind.
	 * @param errorChannel the error channel (if enabled, otherwise null). If not null,
	 * the binder must wire this channel into the producer endpoint so that errors
	 * are forwarded to it.
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception
	 */
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			P producerProperties, MessageChannel channel, MessageChannel errorChannel)
			throws Exception {
		return createProducerMessageHandler(destination, producerProperties, errorChannel);
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
	 *
	 * @param destination the name of the target destination
	 * @param producerProperties the producer properties
	 * @param errorChannel the error channel (if enabled, otherwise null). If not null,
	 * the binder must wire this channel into the producer endpoint so that errors
	 * are forwarded to it.
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception
	 */
	protected abstract MessageHandler createProducerMessageHandler(ProducerDestination destination,
			P producerProperties, MessageChannel errorChannel)
			throws Exception;

	/**
	 * Invoked after the unbinding of a producer. Subclasses may override this to provide
	 * their own logic for dealing with unbinding.
	 *
	 * @param destination the bound destination
	 * @param producerProperties the producer properties
	 */
	protected void afterUnbindProducer(ProducerDestination destination, P producerProperties) {
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
	 *
	 * @param name the name of the destination
	 * @param group the consumer group
	 * @param inputChannel the channel to be bound
	 * @param properties the {@link ConsumerProperties} of the binding
	 * @return the Binding for the channel
	 * @throws BinderException on internal errors during binding
	 */
	@Override
	public final Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel,
			final C properties) throws BinderException {
		MessageProducer consumerEndpoint = null;
		try {
			ConsumerDestination destination = this.provisioningProvider.provisionConsumerDestination(name, group, properties);
			// the function support for the inbound channel is only for Sink
			if (this.streamFunctionProperties != null && StringUtils.hasText(this.streamFunctionProperties.getDefinition()) && this.processor == null) {
				inputChannel = this.postProcessInboundChannelForFunction(inputChannel);
			}
			if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {
				enhanceMessageChannel(inputChannel);
			}
			consumerEndpoint = createConsumerEndpoint(destination, group, properties);
			consumerEndpoint.setOutputChannel(inputChannel);
			if (consumerEndpoint instanceof InitializingBean) {
				((InitializingBean) consumerEndpoint).afterPropertiesSet();
			}
			if (consumerEndpoint instanceof Lifecycle) {
				((Lifecycle) consumerEndpoint).start();
			}

			Binding<MessageChannel> binding = new DefaultBinding<MessageChannel>(name, group, inputChannel,
					consumerEndpoint instanceof Lifecycle ? (Lifecycle) consumerEndpoint : null) {

				@Override
				public Map<String, Object> getExtendedInfo() {
					return doGetExtendedInfo(destination, properties);
				}

				@Override
				protected void afterUnbind() {
					try {
						if (getEndpoint() instanceof DisposableBean) {
							((DisposableBean) getEndpoint()).destroy();
						}
					}
					catch (Exception e) {
						AbstractMessageChannelBinder.this.logger
								.error("Exception thrown while unbinding " + toString(), e);
					}
					afterUnbindConsumer(destination, this.group, properties);
					destroyErrorInfrastructure(destination, group, properties);
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
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name, String group,
			final PollableSource<MessageHandler> inboundBindTarget, C properties) {
		Assert.isInstanceOf(DefaultPollableMessageSource.class, inboundBindTarget);
		DefaultPollableMessageSource bindingTarget = (DefaultPollableMessageSource) inboundBindTarget;
		ConsumerDestination destination = this.provisioningProvider.provisionConsumerDestination(name, group,
				properties);
		if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {
			bindingTarget.addInterceptor(0, this.embeddedHeadersChannelInterceptor);
		}
		final PolledConsumerResources resources = createPolledConsumerResources(name, group, destination, properties);
		bindingTarget.setSource(resources.getSource());
		if (resources.getErrorInfrastructure() != null) {
			if (resources.getErrorInfrastructure().getErrorChannel() != null) {
				bindingTarget.setErrorChannel(resources.getErrorInfrastructure().getErrorChannel());
			}
			ErrorMessageStrategy ems = getErrorMessageStrategy();
			if (ems != null) {
				bindingTarget.setErrorMessageStrategy(ems);
			}
		}
		if (properties.getMaxAttempts() > 1) {
			bindingTarget.setRetryTemplate(buildRetryTemplate(properties));
			bindingTarget.setRecoveryCallback(
					getPolledConsumerRecoveryCallback(resources.getErrorInfrastructure(), properties));
		}
		postProcessPollableSource(bindingTarget);
		if (resources.getSource() instanceof Lifecycle) {
				((Lifecycle) resources.getSource()).start();
		}
		Binding<PollableSource<MessageHandler>> binding = new DefaultBinding<PollableSource<MessageHandler>>(name, group, inboundBindTarget,
				resources.getSource() instanceof Lifecycle ? (Lifecycle) resources.getSource() : null) {

			@Override
			public Map<String, Object> getExtendedInfo() {
				return doGetExtendedInfo(destination, properties);
			}

			@Override
			public void afterUnbind() {
				afterUnbindConsumer(destination, this.group, properties);
				destroyErrorInfrastructure(destination, group, properties);
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
	protected RecoveryCallback<Object> getPolledConsumerRecoveryCallback(ErrorInfrastructure errorInfrastructure,
			C properties) {
		return errorInfrastructure.getRecoverer();
	}

	protected PolledConsumerResources createPolledConsumerResources(String name, String group,
			ConsumerDestination destination, C consumerProperties) {
		throw new UnsupportedOperationException("This binder does not support pollable consumers");
	}

	private void enhanceMessageChannel(MessageChannel inputChannel) {
		((AbstractMessageChannel) inputChannel).addInterceptor(0, this.embeddedHeadersChannelInterceptor);
	}

	/**
	 * Creates {@link MessageProducer} that receives data from the consumer destination.
	 * will be started and stopped by the binder.
	 *
	 * @param group the consumer group
	 * @param destination reference to the consumer destination
	 * @param properties the consumer properties
	 * @return the consumer endpoint.
	 */
	protected abstract MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			C properties) throws Exception;

	/**
	 * Invoked after the unbinding of a consumer. The binder implementation can override
	 * this method to provide their own logic (e.g. for cleaning up destinations).
	 *
	 * @param destination the consumer destination
	 * @param group the consumer group
	 * @param consumerProperties the consumer properties
	 */
	protected void afterUnbindConsumer(ConsumerDestination destination, String group, C consumerProperties) {
	}

	/**
	 * Register an error channel for the destination when an async send error is received.
	 * Bridge the channel to the global error channel (if present).
	 * @param destination the destination.
	 * @return the channel.
	 */
	private SubscribableChannel registerErrorInfrastructure(ProducerDestination destination) {
		ConfigurableListableBeanFactory beanFactory = getApplicationContext().getBeanFactory();
		String errorChannelName = errorsBaseName(destination);
		SubscribableChannel errorChannel = null;
		if (getApplicationContext().containsBean(errorChannelName)) {
			Object errorChannelObject = getApplicationContext().getBean(errorChannelName);
			if (!(errorChannelObject instanceof SubscribableChannel)) {
				throw new IllegalStateException(
						"Error channel '" + errorChannelName + "' must be a SubscribableChannel");
			}
			errorChannel = (SubscribableChannel) errorChannelObject;
		}
		else {
			errorChannel = new PublishSubscribeChannel();
			this.registerComponentWithBeanFactory(errorChannelName, errorChannel);
			errorChannel = (PublishSubscribeChannel) beanFactory.initializeBean(errorChannel, errorChannelName);
		}
		MessageChannel defaultErrorChannel = null;
		if (getApplicationContext().containsBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)) {
			defaultErrorChannel = getApplicationContext().getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
					MessageChannel.class);
		}
		if (defaultErrorChannel != null) {
			BridgeHandler errorBridge = new BridgeHandler();
			errorBridge.setOutputChannel(defaultErrorChannel);
			errorChannel.subscribe(errorBridge);
			String errorBridgeHandlerName = getErrorBridgeName(destination);
			this.registerComponentWithBeanFactory(errorBridgeHandlerName, errorBridge);
			beanFactory.initializeBean(errorBridge, errorBridgeHandlerName);
		}
		return errorChannel;
	}

	/**
	 * Build an errorChannelRecoverer that writes to a pub/sub channel for the destination
	 * when an exception is thrown to a consumer.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @return the ErrorInfrastructure which is a holder for the error channel, the recoverer and the
	 * message handler that is subscribed to the channel.
	 */
	protected final ErrorInfrastructure registerErrorInfrastructure(ConsumerDestination destination, String group,
			C consumerProperties) {

		return registerErrorInfrastructure(destination, group, consumerProperties, false);
	}

	/**
	 * Build an errorChannelRecoverer that writes to a pub/sub channel for the destination
	 * when an exception is thrown to a consumer.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @param polled true if this is for a polled consumer.
	 * @return the ErrorInfrastructure which is a holder for the error channel, the recoverer and the
	 * message handler that is subscribed to the channel.
	 */
	protected final ErrorInfrastructure registerErrorInfrastructure(ConsumerDestination destination, String group,
			C consumerProperties, boolean polled) {

		ErrorMessageStrategy errorMessageStrategy = getErrorMessageStrategy();
		ConfigurableListableBeanFactory beanFactory = getApplicationContext().getBeanFactory();
		String errorChannelName = errorsBaseName(destination, group, consumerProperties);
		SubscribableChannel errorChannel = null;
		if (getApplicationContext().containsBean(errorChannelName)) {
			Object errorChannelObject = getApplicationContext().getBean(errorChannelName);
			if (!(errorChannelObject instanceof SubscribableChannel)) {
				throw new IllegalStateException(
						"Error channel '" + errorChannelName + "' must be a SubscribableChannel");
			}
			errorChannel = (SubscribableChannel) errorChannelObject;
		}
		else {
			errorChannel = new BinderErrorChannel();
			this.registerComponentWithBeanFactory(errorChannelName, errorChannel);
			errorChannel = (LastSubscriberAwareChannel) beanFactory.initializeBean(errorChannel, errorChannelName);
		}
		ErrorMessageSendingRecoverer recoverer;
		if (errorMessageStrategy == null) {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel);
		}
		else {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel, errorMessageStrategy);
		}
		String recovererBeanName = getErrorRecovererName(destination, group, consumerProperties);
		this.registerComponentWithBeanFactory(recovererBeanName, recoverer);
		beanFactory.initializeBean(recoverer, recovererBeanName);
		MessageHandler handler;
		if (polled) {
			handler = getPolledConsumerErrorMessageHandler(destination, group, consumerProperties);
		}
		else {
			handler = getErrorMessageHandler(destination, group, consumerProperties);
		}
		MessageChannel defaultErrorChannel = null;
		if (getApplicationContext().containsBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)) {
			defaultErrorChannel = getApplicationContext().getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
					MessageChannel.class);
		}
		if (handler == null && errorChannel instanceof LastSubscriberAwareChannel) {
			handler = getDefaultErrorMessageHandler((LastSubscriberAwareChannel) errorChannel, defaultErrorChannel != null);
		}
		String errorMessageHandlerName = getErrorMessageHandlerName(destination, group, consumerProperties);
		if (handler != null) {
			this.registerComponentWithBeanFactory(errorMessageHandlerName, handler);
			beanFactory.initializeBean(handler, errorMessageHandlerName);
			errorChannel.subscribe(handler);
		}
		if (defaultErrorChannel != null) {
			BridgeHandler errorBridge = new BridgeHandler();
			errorBridge.setOutputChannel(defaultErrorChannel);
			errorChannel.subscribe(errorBridge);
			String errorBridgeHandlerName = getErrorBridgeName(destination, group, consumerProperties);
			this.registerComponentWithBeanFactory(errorBridgeHandlerName, errorBridge);
			beanFactory.initializeBean(errorBridge, errorBridgeHandlerName);
		}
		return new ErrorInfrastructure(errorChannel, recoverer, handler);
	}

	private void destroyErrorInfrastructure(ProducerDestination destination) {
		String errorChannelName = errorsBaseName(destination);
		String errorBridgeHandlerName = getErrorBridgeName(destination);
		MessageHandler bridgeHandler = null;
		if (getApplicationContext().containsBean(errorBridgeHandlerName)) {
			bridgeHandler = getApplicationContext().getBean(errorBridgeHandlerName, MessageHandler.class);
		}
		if (getApplicationContext().containsBean(errorChannelName)) {
			SubscribableChannel channel = getApplicationContext().getBean(errorChannelName, SubscribableChannel.class);
			if (bridgeHandler != null) {
				channel.unsubscribe(bridgeHandler);
				((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
						.destroySingleton(errorBridgeHandlerName);
			}
			((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
					.destroySingleton(errorChannelName);
		}
	}

	private void destroyErrorInfrastructure(ConsumerDestination destination, String group, C properties) {
		try {
			String recoverer = getErrorRecovererName(destination, group, properties);
			if (getApplicationContext().containsBean(recoverer)) {
				((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory()).destroySingleton(recoverer);
			}
			String errorChannelName = errorsBaseName(destination, group, properties);
			String errorMessageHandlerName = getErrorMessageHandlerName(destination, group, properties);
			String errorBridgeHandlerName = getErrorBridgeName(destination, group, properties);
			MessageHandler bridgeHandler = null;
			if (getApplicationContext().containsBean(errorBridgeHandlerName)) {
				bridgeHandler = getApplicationContext().getBean(errorBridgeHandlerName, MessageHandler.class);
			}
			MessageHandler handler = null;
			if (getApplicationContext().containsBean(errorMessageHandlerName)) {
				handler = getApplicationContext().getBean(errorMessageHandlerName, MessageHandler.class);
			}
			if (getApplicationContext().containsBean(errorChannelName)) {
				SubscribableChannel channel = getApplicationContext().getBean(errorChannelName, SubscribableChannel.class);
				if (bridgeHandler != null) {
					channel.unsubscribe(bridgeHandler);
					((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
							.destroySingleton(errorBridgeHandlerName);
				}
				if (handler != null) {
					channel.unsubscribe(handler);
					((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
							.destroySingleton(errorMessageHandlerName);
				}
				((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory())
						.destroySingleton(errorChannelName);
			}
		}
		catch (IllegalStateException e) {
			// context is shutting down.
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
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination, String group,
			C consumerProperties) {
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
	protected MessageHandler getPolledConsumerErrorMessageHandler(ConsumerDestination destination, String group,
			C consumerProperties) {
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
	protected MessageHandler getDefaultErrorMessageHandler(LastSubscriberAwareChannel errorChannel,
			boolean defaultErrorChannelPresent) {
		return new FinalRethrowingErrorMessageHandler(errorChannel, defaultErrorChannelPresent);
	}

	/**
	 * Binders can return an {@link ErrorMessageStrategy} for building error messages; binder
	 * implementations typically might add extra headers to the error message.
	 * @return the implementation - may be null.
	 */
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return null;
	}

	protected String getErrorRecovererName(ConsumerDestination destination, String group,
			C consumerProperties) {
		return errorsBaseName(destination, group, consumerProperties) + ".recoverer";
	}

	protected String getErrorMessageHandlerName(ConsumerDestination destination, String group,
			C consumerProperties) {
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
		extendedInfo.put(properties.getClass().getSimpleName(), objectMapper.convertValue(properties, Map.class));
		return extendedInfo;
	}

	private void doPublishEvent(ApplicationEvent event) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
	}

	private void registerComponentWithBeanFactory(String name, Object component) {
		if (getApplicationContext().getBeanFactory().containsBean(name)) {
			throw new IllegalStateException("Failed to register bean with name '" + name + "', since bean with the same name already exists. Possible reason: "
					+ "You may have multiple bindings with the same 'destination' and 'group' name (consumer side) "
					+ "and multiple bindings with the same 'destination' name (producer side). Solution: ensure each binding uses different group name (consumer side) "
					+ "or 'destination' name (producer side)." );
		}
		else {
			getApplicationContext().getBeanFactory().registerSingleton(name, component);
		}
	}

	private SubscribableChannel postProcessOutboundChannelForFunction(MessageChannel outputChannel) {
		if (this.integrationFlowFunctionSupport != null) {
			Publisher publisher = MessageChannelReactiveUtils.toPublisher(outputChannel);
			// If the app has an explicit Supplier bean defined, make that as the publisher
			if (this.integrationFlowFunctionSupport.containsFunction(Supplier.class)) {
				IntegrationFlowBuilder integrationFlowBuilder = IntegrationFlows.from(outputChannel).bridge();
				publisher = integrationFlowBuilder.toReactivePublisher();
			}
			if (this.integrationFlowFunctionSupport.containsFunction(Function.class,
					this.streamFunctionProperties.getDefinition())) {
				DirectChannel actualOutputChannel = new DirectChannel();
				if (outputChannel instanceof AbstractMessageChannel) {
					moveChannelInterceptors((AbstractMessageChannel) outputChannel, actualOutputChannel);
				}
				this.integrationFlowFunctionSupport.andThenFunction(publisher, actualOutputChannel,
						this.streamFunctionProperties.getDefinition());
				return actualOutputChannel;
			}
		}
		return (SubscribableChannel) outputChannel;
	}

	private SubscribableChannel postProcessInboundChannelForFunction(MessageChannel inputChannel) {
		if (this.integrationFlowFunctionSupport != null &&
				(this.integrationFlowFunctionSupport.containsFunction(Consumer.class) ||
						this.integrationFlowFunctionSupport.containsFunction(Function.class))) {
			DirectChannel actualInputChannel = new DirectChannel();
			if (inputChannel instanceof AbstractMessageChannel) {
				moveChannelInterceptors((AbstractMessageChannel) inputChannel, actualInputChannel);
			}
			this.integrationFlowFunctionSupport.andThenFunction(MessageChannelReactiveUtils.toPublisher(actualInputChannel),
					inputChannel, this.streamFunctionProperties.getDefinition());
			return actualInputChannel;
		}
		return (SubscribableChannel) inputChannel;
	}

	private void moveChannelInterceptors(AbstractMessageChannel existingMessageChannel,
			AbstractMessageChannel actualMessageChannel) {
		for (ChannelInterceptor channelInterceptor : existingMessageChannel.getChannelInterceptors()) {
			actualMessageChannel.addInterceptor(channelInterceptor);
			existingMessageChannel.removeInterceptor(channelInterceptor);
		}
	}

	private final class SendingHandler extends AbstractMessageHandler implements Lifecycle {

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
		protected void handleMessageInternal(Message<?> message) throws Exception {
			Message<?> messageToSend = (this.useNativeEncoding) ? message
					: serializeAndEmbedHeadersIfApplicable(message);
			this.delegate.handleMessage(messageToSend);
		}


		@SuppressWarnings("deprecation")
		private Message<?> serializeAndEmbedHeadersIfApplicable(Message<?> message) throws Exception {
			MessageValues transformed = serializePayloadIfNecessary(message);
			Object payload;
			if (this.embedHeaders) {
				Object contentType = transformed.get(MessageHeaders.CONTENT_TYPE);
				// transform content type headers to String, so that they can be properly
				// embedded in JSON
				if (contentType != null) {
					transformed.put(MessageHeaders.CONTENT_TYPE, contentType.toString());
				}
				Object originalContentType = transformed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
				if (originalContentType != null) {
					transformed.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
				}
				payload = EmbeddedHeaderUtils.embedHeaders(transformed, this.embeddedHeaders);
			}
			else {
				payload = transformed.getPayload();
			}
			return getMessageBuilderFactory().withPayload(payload).copyHeaders(transformed.getHeaders()).build();
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

	protected static class ErrorInfrastructure {

		private final SubscribableChannel errorChannel;

		private final ErrorMessageSendingRecoverer recoverer;

		private final MessageHandler handler;

		ErrorInfrastructure(SubscribableChannel errorChannel, ErrorMessageSendingRecoverer recoverer,
				MessageHandler handler) {
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

	private static final class EmbeddedHeadersChannelInterceptor implements ChannelInterceptor {

		protected final Log logger;

		EmbeddedHeadersChannelInterceptor(Log logger) {
			this.logger = logger;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			if (message.getPayload() instanceof byte[] &&
					!message.getHeaders().containsKey(BinderHeaders.NATIVE_HEADERS_PRESENT)
					&& EmbeddedHeaderUtils.mayHaveEmbeddedHeaders((byte[]) message.getPayload())) {

				MessageValues messageValues;
				try {
					messageValues = EmbeddedHeaderUtils.extractHeaders((Message<byte[]>) message, true);
				}
				catch (Exception e) {
					/*
					 * debug() rather then error() since we don't know for sure that it
					 * really is a message with embedded headers, it just meets the
					 * criteria in EmbeddedHeaderUtils.mayHaveEmbeddedHeaders().
					 */
					if (logger.isDebugEnabled()) {
						logger.debug(EmbeddedHeaderUtils.decodeExceptionMessage(message), e);
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

		public PolledConsumerResources(MessageSource<?> source, ErrorInfrastructure errorInfrastructure) {
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
}
