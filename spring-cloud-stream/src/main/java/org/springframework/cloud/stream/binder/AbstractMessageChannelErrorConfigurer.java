/*
 * Copyright 2017 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base implementation of {@link BinderErrorConfigurer}, provides default implementations for
 * {@link Binder} implementations based on {@link MessageChannel} abstractions.
 * Binder implementations are still responsible to subclass this and provide a configure implementation
 * that fits their specific needs.
 * @author Vinicius Carvalho
 * @author Gary Russel
 */
public abstract class AbstractMessageChannelErrorConfigurer<C extends ConsumerProperties> implements BinderErrorConfigurer<C,MessageChannel>, ApplicationContextAware{

	private volatile AbstractApplicationContext applicationContext;

	private Map<String,ErrorInfrastructure> destinationErrors = new ConcurrentHashMap<>();

	protected final Log logger = LogFactory.getLog(getClass());

	protected AbstractApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(AbstractApplicationContext.class, applicationContext);
		this.applicationContext = (AbstractApplicationContext) applicationContext;
	}

	@Override
	public void register(Binding<MessageChannel> binding, String group, C consumerProperties) {
		MessageProducerBinding consumerBinding = (MessageProducerBinding)binding;
		ErrorMessageStrategy errorMessageStrategy = getErrorMessageStrategy();
		ConfigurableListableBeanFactory beanFactory = getApplicationContext().getBeanFactory();
		String errorChannelName = errorsBaseName(consumerBinding.getDestination(), group, consumerProperties);
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
			beanFactory.registerSingleton(errorChannelName, errorChannel);
			errorChannel = (LastSubscriberAwareChannel) beanFactory.initializeBean(errorChannel, errorChannelName);
		}
		ErrorMessageSendingRecoverer recoverer;
		if (errorMessageStrategy == null) {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel);
		}
		else {
			recoverer = new ErrorMessageSendingRecoverer(errorChannel, errorMessageStrategy);
		}
		String recovererBeanName = getErrorRecovererName(consumerBinding.getDestination(), group, consumerProperties);
		beanFactory.registerSingleton(recovererBeanName, recoverer);
		beanFactory.initializeBean(recoverer, recovererBeanName);
		MessageHandler handler = getErrorMessageHandler(consumerBinding.getDestination().getName(), group, consumerProperties);
		MessageChannel defaultErrorChannel = null;
		if (getApplicationContext().containsBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)) {
			defaultErrorChannel = getApplicationContext().getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
					MessageChannel.class);
		}
		if (handler == null && errorChannel instanceof LastSubscriberAwareChannel) {
			handler = getDefaultErrorMessageHandler((LastSubscriberAwareChannel) errorChannel, defaultErrorChannel != null);
		}
		String errorMessageHandlerName = getErrorMessageHandlerName(consumerBinding.getDestination(), group, consumerProperties);
		if (handler != null) {
			beanFactory.registerSingleton(errorMessageHandlerName, handler);
			beanFactory.initializeBean(handler, errorMessageHandlerName);
			errorChannel.subscribe(handler);
		}
		if (defaultErrorChannel != null) {
			BridgeHandler errorBridge = new BridgeHandler();
			errorBridge.setOutputChannel(defaultErrorChannel);
			errorChannel.subscribe(errorBridge);
			String errorBridgeHandlerName = getErrorBridgeName(consumerBinding.getDestination(), group, consumerProperties);
			beanFactory.registerSingleton(errorBridgeHandlerName, errorBridge);
			beanFactory.initializeBean(errorBridge, errorBridgeHandlerName);
		}
		destinationErrors.put(consumerBinding.getDestination().getName(),new ErrorInfrastructure(errorChannel,recoverer,handler));
	}

	@Override
	public void destroy(Binding<MessageChannel> binding, String group, C consumerProperties) {
		try {
			MessageProducerBinding consumerBinding = (MessageProducerBinding)binding;
			String recoverer = getErrorRecovererName(consumerBinding.getDestination(), group, consumerProperties);
			if (getApplicationContext().containsBean(recoverer)) {
				((DefaultSingletonBeanRegistry) getApplicationContext().getBeanFactory()).destroySingleton(recoverer);
			}
			String errorChannelName = errorsBaseName(consumerBinding.getDestination(), group, consumerProperties);
			String errorMessageHandlerName = getErrorMessageHandlerName(consumerBinding.getDestination(), group, consumerProperties);
			String errorBridgeHandlerName = getErrorBridgeName(consumerBinding.getDestination(), group, consumerProperties);
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
			destinationErrors.remove(consumerBinding.getDestination().getName());
		}
		catch (IllegalStateException e) {
			// context is shutting down.
		}
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
		return destination.getName() + ".errors";
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
	 * Binders can return a message handler to be subscribed to the error channel.
	 * Examples might be if the user wishes to (re)publish messages to a DLQ.
	 * @param destination the destination.
	 * @param group the group.
	 * @param consumerProperties the properties.
	 * @return the handler (may be null, which is the default, causing the exception to be
	 * rethrown).
	 */
	protected MessageHandler getErrorMessageHandler(final String destination, final String group,
			final C consumerProperties) {
		return null;
	}

	public ErrorInfrastructure getErrorInfrastructure(String destinationName){
		return destinationErrors.get(destinationName);
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

	public RetryTemplate buildRetryTemplate(ConsumerProperties properties) {
		RetryTemplate template = new RetryTemplate();
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(properties.getMaxAttempts());
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval());
		backOffPolicy.setMultiplier(properties.getBackOffMultiplier());
		backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval());
		template.setRetryPolicy(retryPolicy);
		template.setBackOffPolicy(backOffPolicy);
		return template;
	}
}
