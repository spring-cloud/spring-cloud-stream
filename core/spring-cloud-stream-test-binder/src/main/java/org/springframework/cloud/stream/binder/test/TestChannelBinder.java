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

package org.springframework.cloud.stream.binder.test;

import java.util.function.Consumer;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.test.TestChannelBinderProvisioner.SpringIntegrationConsumerDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderProvisioner.SpringIntegrationProducerDestination;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.core.retry.RetryException;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.integration.support.MapBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.integration.core.RecoveryCallback;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link Binder} backed by the Spring Integration framework. It is useful
 * for localized demos and testing.
 * <p>
 * This binder extends from the same base class ({@link AbstractMessageChannelBinder}) as
 * other binders (i.e., Rabbit, Kafka etc.). Interaction with this binder is done via
 * source and target destination which emulate real binder's destinations (i.e., Kafka
 * topic) <br>
 * The destination classes are
 * <ul>
 * <li>{@link InputDestination}</li>
 * <li>{@link OutputDestination}</li>
 * </ul>
 * Simply autowire them in your application and send/receive messages.
 * </p>
 * You must also add {@link TestChannelBinderConfiguration} to your configuration. Below
 * is the example using Spring Boot test. <pre class="code">
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
 *         sourceDestination.send(new GenericMessage&lt;String&gt;("Hello"));
 *         assertEquals("Hello world",
 *             new String((byte[])targetDestination.receive().getPayload(), StandardCharsets.UTF_8));
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
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public class TestChannelBinder extends
	AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, TestChannelBinderProvisioner> {

	@Autowired
	private BeanFactory beanFactory;

	private Message<?> lastError;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private MessageSource<?> messageSourceDelegate = () -> new GenericMessage<>(
		"polled data", new MapBuilder()
		.put(MessageHeaders.CONTENT_TYPE, "text/plain")
		.put(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, (AcknowledgmentCallback) status -> {
		}).get());

	public TestChannelBinder(TestChannelBinderProvisioner provisioningProvider) {
		super(new String[] {}, provisioningProvider);
	}

	/**
	 * Set a delegate {@link MessageSource} for pollable consumers.
	 * @param messageSourceDelegate the delegate.
	 */
	@Autowired(required = false)
	public void setMessageSourceDelegate(MessageSource<byte[]> messageSourceDelegate) {
		this.messageSourceDelegate = messageSourceDelegate;
	}

	public Message<?> getLastError() {
		return this.lastError;
	}

	public void resetLastError() {
		this.lastError = null;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
														ProducerProperties producerProperties, MessageChannel errorChannel)
		throws Exception {
		BridgeHandler handler = new BridgeHandler();
		handler.setBeanFactory(this.beanFactory);
		handler.setOutputChannel(
			((SpringIntegrationProducerDestination) destination).getChannel());
		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
													String group, ConsumerProperties properties) throws Exception {
		ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();
		SubscribableChannel siBinderInputChannel = ((SpringIntegrationConsumerDestination) destination)
			.getChannel();

		IntegrationMessageListeningContainer messageListenerContainer = new IntegrationMessageListeningContainer();
		IntegrationBinderInboundChannelAdapter adapter = new IntegrationBinderInboundChannelAdapter(
			messageListenerContainer);
		adapter.setBeanFactory(this.beanFactory);
		String groupName = StringUtils.hasText(group) ? group : "anonymous";
		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination,
			groupName, properties);
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

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name,
																	String group, ConsumerDestination destination,
																	ConsumerProperties consumerProperties) {
		return new PolledConsumerResources(this.messageSourceDelegate,
			registerErrorInfrastructure(destination, group, consumerProperties));
	}

	@Override
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination,
													String group, ConsumerProperties consumerProperties) {
		return m -> {
			this.logger.info("Error handled: " + m);
			this.lastError = m;
		};
	}

	/**
	 * Implementation of a simple message listener container modeled after AMQP
	 * SimpleMessageListenerContainer.
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
	 * Implementation of an inbound channel adapter modeled after AmqpInboundChannelAdapter.
	 */
	private static class IntegrationBinderInboundChannelAdapter
		extends MessageProducerSupport {

		private final IntegrationMessageListeningContainer listenerContainer;

		private RetryTemplate retryTemplate;

		private RecoveryCallback<?> recoveryCallback;

		IntegrationBinderInboundChannelAdapter(
			IntegrationMessageListeningContainer listenerContainer) {
			this.listenerContainer = listenerContainer;
		}

		// Temporarily unused until DLQ strategy for this binder becomes a requirement
		public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
			this.recoveryCallback = recoveryCallback;
		}

		public void setRetryTemplate(RetryTemplate retryTemplate) {
			this.retryTemplate = retryTemplate;
		}

		@Override
		protected void onInit() {
			if (this.retryTemplate != null) {
				Assert.state(getErrorChannel() == null,
					"Cannot have an 'errorChannel' property when a 'RetryTemplate' is "
						+ "provided; use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to "
						+ "send an error message when retries are exhausted");
			}
			Listener messageListener = new Listener();
			this.listenerContainer.setMessageListener(messageListener);
		}

		protected class Listener implements Consumer<Message<?>> {

			@Override
			public void accept(Message<?> message) {
				try {
					if (IntegrationBinderInboundChannelAdapter.this.retryTemplate == null) {
						processMessage(message);
					}
					else {
						try {
							IntegrationBinderInboundChannelAdapter.this.retryTemplate.execute(() -> {
									processMessage(message);
									return null;
								});
						}
						catch (RetryException ex) {
							IntegrationBinderInboundChannelAdapter.this.recoveryCallback
								.recover(ErrorMessageUtils.getAttributeAccessor(message, null), ex);
						}
					}
				}
				catch (RuntimeException e) {
					if (getErrorChannel() != null) {
						getMessagingTemplate()
							.send(getErrorChannel(),
								buildErrorMessage(null, new IllegalStateException(
									"Message conversion failed: " + message,
									e)));
					}
					else {
						throw e;
					}
				}
			}

			private void processMessage(Message<?> message) {
				sendMessage(message);
			}

		}

	}

}
