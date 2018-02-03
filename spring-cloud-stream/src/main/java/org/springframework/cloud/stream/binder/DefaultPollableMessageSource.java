/*
 * Copyright 2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.context.Lifecycle;
import org.springframework.core.AttributeAccessor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.support.AckUtils;
import org.springframework.integration.support.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.integration.support.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * The default implementation of a {@link PollableMessageSource}.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DefaultPollableMessageSource implements PollableMessageSource, Lifecycle, RetryListener {

	protected static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<AttributeAccessor>();

	private final List<ChannelInterceptor> interceptors = new ArrayList<>();

	private final MessagingTemplate messagingTemplate = new MessagingTemplate();

	private MessageSource<?> source;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<Object> recoveryCallback;

	private MessageChannel errorChannel;

	private ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();

	private BiConsumer<AttributeAccessor, Message<?>> attributesProvider;

	private SmartMessageConverter messageConverter;

	private boolean running;

	public void setSource(MessageSource<?> source) {
		ProxyFactory pf = new ProxyFactory(source);
		class ReceiveAdvice implements MethodInterceptor {

			private final List<ChannelInterceptor> interceptors = new ArrayList<>();

			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (result instanceof Message) {
					Message<?> received = (Message<?>) result;
					for (ChannelInterceptor interceptor : this.interceptors) {
						received = interceptor.preSend(received, null);
						if (received == null) {
							return null;
						}
					}
					return received;
				}
				return result;
			}

		}
		final ReceiveAdvice advice = new ReceiveAdvice();
		advice.interceptors.addAll(this.interceptors);
		NameMatchMethodPointcutAdvisor sourceAdvisor = new NameMatchMethodPointcutAdvisor(advice);
		sourceAdvisor.addMethodName("receive");
		pf.addAdvisor(sourceAdvisor);
		this.source = (MessageSource<?>) pf.getProxy();
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		retryTemplate.registerListener(this);
		this.retryTemplate = retryTemplate;
	}

	public void setRecoveryCallback(RecoveryCallback<Object> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	public void setErrorChannel(MessageChannel errorChannel) {
		this.errorChannel = errorChannel;
	}

	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		Assert.notNull(errorMessageStrategy, "'errorMessageStrategy' cannot be null");
		this.errorMessageStrategy = errorMessageStrategy;
	}

	public void setAttributesProvider(BiConsumer<AttributeAccessor, Message<?>> attributesProvider) {
		this.attributesProvider = attributesProvider;
	}

	public void setMessageConverter(SmartMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	public void addInterceptor(ChannelInterceptor interceptor) {
		this.interceptors.add(interceptor);
	}

	public void addInterceptor(int index, ChannelInterceptor interceptor) {
		this.interceptors.add(index, interceptor);
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public synchronized void start() {
		if (!this.running && this.source instanceof Lifecycle) {
			((Lifecycle) this.source).start();
		}
		this.running = true;
	}

	@Override
	public synchronized void stop() {
		if (this.running && this.source instanceof Lifecycle) {
			((Lifecycle) this.source).stop();
		}
		this.running = false;
	}

	/**
	 * If there's a retry template, it will set the attributes holder via the listener. If
	 * there's no retry template, but there's an error channel, we create a new attributes
	 * holder here. If an attributes holder exists (by either method), we set the
	 * attributes for use by the {@link ErrorMessageStrategy}.
	 * @param message the Spring Messaging message to use.
	 */
	private void setAttributesIfNecessary(Message<?> message) {
		boolean needHolder = this.errorChannel != null && this.retryTemplate == null;
		boolean needAttributes = needHolder || this.retryTemplate != null;
		if (needHolder) {
			attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(null, null));
		}
		if (needAttributes) {
			AttributeAccessor attributes = attributesHolder.get();
			if (attributes != null) {
				attributes.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, message);
				if (this.attributesProvider != null) {
					this.attributesProvider.accept(attributes, message);
				}
			}
		}
	}

	@Override
	public boolean poll(MessageHandler handler) {
		return poll(handler, null);
	}

	@Override
	public boolean poll(MessageHandler handler, ParameterizedTypeReference<?> type) {
		Message<?> message = this.source.receive();
		if (message == null) {
			return false;
		}
		if (type != null && this.messageConverter != null) {
			Object payload = this.messageConverter.fromMessage(message, byte[].class, type);
			message = MessageBuilder.withPayload(payload)
					.copyHeaders(message.getHeaders())
					.build();
		}
		AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor
				.getAcknowledgmentCallback(message);
		try {
			if (this.retryTemplate == null && this.errorChannel == null) {
				setAttributesIfNecessary(message);
				doHandleMessage(handler, message);
			}
			else if (this.retryTemplate == null) {
				try {
					setAttributesIfNecessary(message);
					doHandleMessage(handler, message);
				}
				catch (Exception e) {
					if (this.errorChannel != null) {
						this.messagingTemplate.send(this.errorChannel,
								this.errorMessageStrategy.buildErrorMessage(e, attributesHolder.get()));
					}
					else {
						throw e;
					}
				}
			}
			else {
				final Message<?> messageToHandle = message;
				this.retryTemplate.execute(context -> {
					setAttributesIfNecessary(messageToHandle);
					doHandleMessage(handler, messageToHandle);
					return null;
				}, this.recoveryCallback);
			}
			return true;
		}
		catch (Exception e) {
			AckUtils.autoNack(ackCallback);
			if (e instanceof MessageHandlingException
					&& ((MessageHandlingException) e).getFailedMessage().equals(message)) {
				throw (MessageHandlingException) e;
			}
			throw new MessageHandlingException(message, e);
		}
		finally {
			AckUtils.autoAck(ackCallback);
		}
	}

	private void doHandleMessage(MessageHandler handler, Message<?> message) {
		try {
			handler.handleMessage(message);
		}
		catch (Throwable t) { // NOSONAR
			throw new MessageHandlingException(message, t);
		}
	}

	@Override
	public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
		if (DefaultPollableMessageSource.this.recoveryCallback != null) {
			attributesHolder.set(context);
		}
		return true;
	}

	@Override
	public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {
		attributesHolder.remove();
	}

	@Override
	public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {
		// Empty
	}

}
