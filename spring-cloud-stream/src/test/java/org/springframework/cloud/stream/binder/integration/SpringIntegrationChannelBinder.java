/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.integration;

import java.util.function.Consumer;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner.SpringIntegrationConsumerDestination;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner.SpringIntegrationProducerDestination;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link Binder} backed by Spring Integration framework.
 * It is useful for localized demos and testing.
 * <p>
 * This binder extends from the same base class ({@link AbstractMessageChannelBinder}) as
 * other binders (i.e., Rabbit, Kafka etc). Interaction with this binder is done via source
 * and target destination which emulate real binder's destinations (i.e., Kafka topic)
 * <br>
 * The destination classes are
 * <ul>
 * <li>{@link SourceDestination}</li>
 * <li>{@link TargetDestination}</li>
 * </ul>
 * Simply autowire them in your your application and send/receive messages.
 * </p>
 * You must also add {@link SpringIntegrationBinderConfiguration} to your configuration.
 * Below is the example using Spring Boot test.
 * <pre class="code">
 *
 * &#064;RunWith(SpringJUnit4ClassRunner.class)
 * &#064;SpringBootTest(classes = {SpringIntegrationBinderConfiguration.class, TestWithSIBinder.MyProcessor.class})
 * public class TestWithSIBinder {
 *     &#064;Autowired
 *     private SourceDestination sourceDestination;
 *
 *     &#064;Autowired
 *     private TargetDestination targetDestination;
 *
 *     &#064;Test
 *     public void testWiring() {
 *         sourceDestination.send(new GenericMessage<String>("Hello"));
 *         assertEquals("Hello world", new String((byte[])targetDestination.receive().getPayload(), StandardCharsets.UTF_8));
 *     }
 *
 *     &#064;SpringBootApplication
 *     &#064;EnableBinding(Processor.class)
 *     public static class MyProcessor {
 *         &#064;StreamListener(Processor.INPUT)
 *         &#064;SendTo(Processor.OUTPUT)
 *         public String transform(String in) {
 *             return in + " world";
 *         }
 *     }
 * }
 * </pre>
 *
 * @author Oleg Zhurakousky
 */
class SpringIntegrationChannelBinder extends AbstractMessageChannelBinder<ConsumerProperties,
	ProducerProperties, SpringIntegrationProvisioner> {

	@Autowired
	private BeanFactory beanFactory;

	SpringIntegrationChannelBinder(SpringIntegrationProvisioner provisioningProvider) {
		super(true, new String[] {}, provisioningProvider);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ProducerProperties producerProperties, MessageChannel errorChannel) throws Exception {
		BridgeHandler handler = new BridgeHandler();
		handler.setBeanFactory(this.beanFactory);
		handler.setOutputChannel(((SpringIntegrationProducerDestination)destination).getChannel());
		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group, ConsumerProperties properties)
			throws Exception {
		ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();
		SubscribableChannel siBinderInputChannel = ((SpringIntegrationConsumerDestination)destination).getChannel();

		IntegrationMessageListeningContainer messageListenerContainer = new IntegrationMessageListeningContainer();
		IntegrationBinderInboundChannelAdapter adapter = new IntegrationBinderInboundChannelAdapter(messageListenerContainer);

		String groupName = StringUtils.hasText(group) ? group : "anonymous";
		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination, groupName, properties);
		if (properties.getMaxAttempts() > 1) {
			adapter.setRetryTemplate(buildRetryTemplate(properties));
			adapter.setRecoveryCallback(errorInfrastructure.getRecoverer());
		}
		else {
			adapter.setErrorMessageStrategy(errorMessageStrategy);
			adapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		}

		siBinderInputChannel.subscribe(messageListenerContainer);

		return adapter;
	}

	/**
	 * Implementation of simple message listener container modeled after AMQP SimpleMessageListenerContainer
	 */
	private static class IntegrationMessageListeningContainer implements MessageHandler {

		private Consumer<Message<?>> listener;

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			this.listener.accept(message);
		}

		public void setMessageListener(Consumer<Message<?>> listener) {
			this.listener = listener;
		}
	}

	/**
	 * Implementation of inbound channel adapter modeled after AmqpInboundChannelAdapter
	 */
	private static class IntegrationBinderInboundChannelAdapter extends MessageProducerSupport {

		private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<AttributeAccessor>();

		private final IntegrationMessageListeningContainer listenerContainer;

		private RetryTemplate retryTemplate;

		private RecoveryCallback<? extends Object> recoveryCallback;

		IntegrationBinderInboundChannelAdapter(IntegrationMessageListeningContainer listenerContainer) {
			this.listenerContainer = listenerContainer;
		}

		@SuppressWarnings("unused")
		// Temporarily unused until DLQ strategy for this binder becomes a requirement
		public void setRecoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
			this.recoveryCallback = recoveryCallback;
		}

		public void setRetryTemplate(RetryTemplate retryTemplate) {
			this.retryTemplate = retryTemplate;
		}

		@Override
		protected void onInit() {
			if (this.retryTemplate != null) {
				Assert.state(getErrorChannel() == null, "Cannot have an 'errorChannel' property when a 'RetryTemplate' is "
						+ "provided; use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to "
						+ "send an error message when retries are exhausted");
			}
			Listener messageListener = new Listener();
			if (this.retryTemplate != null) {
				this.retryTemplate.registerListener(messageListener);
			}
			this.listenerContainer.setMessageListener(messageListener);
		}

		protected class Listener implements RetryListener, Consumer<Message<?>> {

			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public void accept(final Message<?> message) {
				try {
					if (IntegrationBinderInboundChannelAdapter.this.retryTemplate == null) {
						try {
							processMessage(message);
						}
						finally {
							attributesHolder.remove();
						}
					}
					else {
						try {
							IntegrationBinderInboundChannelAdapter.this.retryTemplate.execute(new RetryCallback() {

								@Override
								public Object doWithRetry(RetryContext context) throws Throwable {
									processMessage(message);
									return null;
								}
							},(RecoveryCallback<Object>) IntegrationBinderInboundChannelAdapter.this.recoveryCallback);
						}
						catch (Throwable e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				catch (RuntimeException e) {
					if (getErrorChannel() != null) {
						getMessagingTemplate().send(getErrorChannel(), buildErrorMessage(null,
								new IllegalStateException("Message conversion failed: " + message, e)));
					}
					else {
						throw e;
					}
				}
			}

			private void processMessage(Message<?> message) {
				sendMessage(message);
			}

			@Override
			public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
				if (IntegrationBinderInboundChannelAdapter.this.recoveryCallback != null) {
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
	}
}
