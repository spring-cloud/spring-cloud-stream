/*
 * Copyright 2013-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.Deflater;

import com.rabbitmq.client.LongString;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.MessageBatch;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.CorrelationData.Confirm;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.AsyncConsumerStartedEvent;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.RequeueCurrentMessageException;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties.QuorumConfig;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties.ContainerType;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.AlternateExchange;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.NackedAmqpMessageException;
import org.springframework.integration.amqp.support.ReturnedAmqpMessageException;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author David Turanski
 * @author Artem Bilan
 * @author Chris Bono
 */
// @checkstyle:off
class RabbitBinderTests extends
		PartitionCapableBinderTests<RabbitTestBinder, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> {

	protected static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	private static final String CLASS_UNDER_TEST_NAME = RabbitMessageChannelBinder.class.getSimpleName();

	private static final String TEST_PREFIX = "bindertest.";

	private static final String BIG_EXCEPTION_MESSAGE = new String(new byte[10_000]).replaceAll("\u0000", "x");

	private static final WebClient client;

	private static final URI uri;

	static {
		client = WebClient.builder()
				.filter(ExchangeFilterFunctions
						.basicAuthentication(RABBITMQ.getAdminUsername(), RABBITMQ.getAdminPassword()))
				.build();
		try {
			uri = new URI(RABBITMQ.getHttpUrl() + "/api/");
		}
		catch (URISyntaxException ex) {
			throw new IllegalStateException(ex);
		}
	}

	@RegisterExtension
	private final RabbitTestSupport rabbitTestSupport = new RabbitTestSupport(true, RABBITMQ.getAmqpPort(), RABBITMQ.getHttpPort());

	private int maxStackTraceSize;

	@Override
	protected RabbitTestBinder getBinder() {
		if (this.testBinder == null) {
			RabbitProperties rabbitProperties = new RabbitProperties();
			this.testBinder = new RabbitTestBinder(this.rabbitTestSupport.getResource(), rabbitProperties);
		}
		return this.testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<RabbitConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new RabbitConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<RabbitProducerProperties> createProducerProperties(TestInfo testInfo) {
		ExtendedProducerProperties<RabbitProducerProperties> props = new ExtendedProducerProperties<>(
				new RabbitProducerProperties());

		if (testInfo.getTestMethod().get().getName().equals("testPartitionedModuleSpEL")) {
			props.getExtension().setRoutingKeyExpression(
					spelExpressionParser.parseExpression("'part.0'"));
		}
		return props;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	void sendAndReceiveBad(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		final AtomicReference<AsyncConsumerStartedEvent> event = new AtomicReference<>();
		binder.getApplicationContext().addApplicationListener(
				(ApplicationListener<AsyncConsumerStartedEvent>) e -> event.set(e));
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties(testInfo));
		assertThat(TestUtils.getPropertyValue(producerBinding,
				"lifecycle.headersMappedLast", Boolean.class)).isTrue();
		assertThat(
				TestUtils
						.getPropertyValue(producerBinding,
								"lifecycle.amqpTemplate.messageConverter")
						.getClass().getName()).contains("Passthrough");
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProps = createConsumerProperties();
		consumerProps.getExtension().setContainerType(ContainerType.DIRECT);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, consumerProps);
		assertThat(
				TestUtils.getPropertyValue(consumerBinding, "lifecycle.messageConverter")
						.getClass().getName()).contains("Passthrough");
		assertThat(TestUtils.getPropertyValue(consumerBinding,
				"lifecycle.messageListenerContainer"))
						.isInstanceOf(DirectMessageListenerContainer.class);
		Message<?> message = MessageBuilder.withPayload("bad".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
		final CountDownLatch latch = new CountDownLatch(3);
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
				throw new RuntimeException("bad");
			}
		});
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(event.get()).isNotNull();
		producerBinding.unbind();
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void producerErrorChannel(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		CachingConnectionFactory ccf = this.rabbitTestSupport.getResource();
		ccf.setPublisherReturns(true);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.resetConnection();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		ExtendedProducerProperties<RabbitProducerProperties> producerProps = createProducerProperties(testInfo);
		producerProps.setErrorChannelEnabled(true);
		producerProps.populateBindingName("output");
		Binding<MessageChannel> producerBinding = binder.bindProducer("ec.0",
				moduleOutputChannel, producerProps);
		final Message<?> message = MessageBuilder.withPayload("bad".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
		String s = testBinder.getBinder().getBinderIdentity() + ".output.errors";
		SubscribableChannel ec = binder.getApplicationContext().getBean(s,
				SubscribableChannel.class);
		final AtomicReference<Message<?>> errorMessage = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(2);
		ec.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				errorMessage.set(message);
				latch.countDown();
			}

		});
		SubscribableChannel globalEc = binder.getApplicationContext().getBean(
				IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				SubscribableChannel.class);
		globalEc.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
			}

		});
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorMessage.get()).isInstanceOf(ErrorMessage.class);
		assertThat(errorMessage.get().getPayload())
				.isInstanceOf(ReturnedAmqpMessageException.class);
		ReturnedAmqpMessageException exception = (ReturnedAmqpMessageException) errorMessage
				.get().getPayload();
		assertThat(exception.getReplyCode()).isEqualTo(312);
		assertThat(exception.getReplyText()).isEqualTo("NO_ROUTE");

		AmqpOutboundEndpoint endpoint = TestUtils.getPropertyValue(producerBinding,
				"lifecycle", AmqpOutboundEndpoint.class);
		assertThat(TestUtils.getPropertyValue(endpoint,
				"confirmCorrelationExpression.expression")).isEqualTo("#root");
		class WrapperAccessor extends AmqpOutboundEndpoint {

			WrapperAccessor(AmqpTemplate amqpTemplate) {
				super(amqpTemplate);
			}

			CorrelationDataWrapper getWrapper() throws Exception {
				Constructor<CorrelationDataWrapper> constructor = CorrelationDataWrapper.class
						.getDeclaredConstructor(String.class, Object.class,
								Message.class);
				ReflectionUtils.makeAccessible(constructor);
				return constructor.newInstance(UUID.randomUUID().toString(), message, message);
			}

		}
		endpoint.confirm(new WrapperAccessor(mock(AmqpTemplate.class)).getWrapper(),
				false, "Mock NACK");
		assertThat(errorMessage.get()).isInstanceOf(ErrorMessage.class);
		assertThat(errorMessage.get().getPayload())
				.isInstanceOf(NackedAmqpMessageException.class);
		NackedAmqpMessageException nack = (NackedAmqpMessageException) errorMessage.get()
				.getPayload();
		assertThat(nack.getNackReason()).isEqualTo("Mock NACK");
		assertThat(nack.getCorrelationData()).isEqualTo(message);
		assertThat(nack.getFailedMessage()).isEqualTo(message);
		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void producerAckChannel(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		CachingConnectionFactory ccf = this.rabbitTestSupport.getResource();
		ccf.setPublisherReturns(true);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.resetConnection();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		ExtendedProducerProperties<RabbitProducerProperties> producerProps = createProducerProperties(testInfo);
		producerProps.setErrorChannelEnabled(true);
		producerProps.getExtension().setConfirmAckChannel("acksChannel");
		Binding<MessageChannel> producerBinding = binder.bindProducer("acks.0",
				moduleOutputChannel, producerProps);
		final Message<?> message = MessageBuilder.withPayload("acksMessage".getBytes())
				.build();
		final AtomicReference<Message<?>> confirm = new AtomicReference<>();
		final CountDownLatch confirmLatch = new CountDownLatch(1);
		binder.getApplicationContext().getBean("acksChannel", DirectChannel.class)
				.subscribe(m -> {
					confirm.set(m);
					confirmLatch.countDown();
				});
		moduleOutputChannel.send(message);
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(confirm.get().getPayload()).isEqualTo("acksMessage".getBytes());
		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void producerConfirmHeader(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		CachingConnectionFactory ccf = this.rabbitTestSupport.getResource();
		ccf.setPublisherReturns(true);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.resetConnection();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		ExtendedProducerProperties<RabbitProducerProperties> producerProps = createProducerProperties(testInfo);
		producerProps.getExtension().setUseConfirmHeader(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer("confirms.0",
				moduleOutputChannel, producerProps);
		CorrelationData correlation = new CorrelationData("testConfirm");
		final Message<?> message = MessageBuilder.withPayload("confirmsMessage".getBytes())
				.setHeader(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, correlation)
				.build();
		moduleOutputChannel.send(message);
		Confirm confirm = correlation.getFuture().get(10, TimeUnit.SECONDS);
		assertThat(confirm.isAck()).isTrue();
		assertThat(correlation.getReturned()).isNotNull();
		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void producerWithAlternateExchange(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		ExtendedProducerProperties<RabbitProducerProperties> producerProps = createProducerProperties(testInfo);
		AlternateExchange alternate = new AlternateExchange();
		alternate.setName("altEx");
		RabbitProducerProperties.AlternateExchange.Binding binding =
				new RabbitProducerProperties.AlternateExchange.Binding();
		binding.setQueue("altQ");
		alternate.setBinding(binding);
		producerProps.getExtension().setAlternateExchange(alternate);
		Binding<MessageChannel> producerBinding = binder.bindProducer("alt.0",
				moduleOutputChannel, producerProps);
		final Message<?> message = MessageBuilder.withPayload("altMessage".getBytes())
				.build();
		moduleOutputChannel.send(message);
		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
		RabbitTemplate template = new RabbitTemplate(this.rabbitTestSupport.getResource());
		Object received = template.receiveAndConvert("altQ", 10_000);
		assertThat(received).isEqualTo("altMessage".getBytes());
		RabbitAdmin admin = new RabbitAdmin(template.getConnectionFactory());
		admin.deleteQueue("altQ");
		admin.deleteExchange("altEx");
	}

	@Test
	void consumerProperties() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setRequeueRejected(true);
		properties.getExtension().setTransacted(true);
		properties.getExtension().setExclusive(true);
		properties.getExtension().setMissingQueuesFatal(true);
		properties.getExtension().setFailedDeclarationRetryInterval(1500L);
		properties.getExtension().setQueueDeclarationRetries(23);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("props.0", null,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
		assertThat(container.getQueueNames()[0])
				.startsWith(properties.getExtension().getPrefix());
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class))
				.isTrue();
		assertThat(TestUtils.getPropertyValue(container, "exclusive", Boolean.class))
				.isTrue();
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers"))
				.isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers"))
				.isNull();
		assertThat(TestUtils.getPropertyValue(container, "defaultRequeueRejected",
				Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "prefetchCount")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "batchSize")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "missingQueuesFatal",
				Boolean.class)).isTrue();
		assertThat(
				TestUtils.getPropertyValue(container, "failedDeclarationRetryInterval"))
						.isEqualTo(1500L);
		assertThat(TestUtils.getPropertyValue(container, "declarationRetries"))
				.isEqualTo(23);
		RetryTemplate retry = TestUtils.getPropertyValue(endpoint, "retryTemplate",
				RetryTemplate.class);
		assertThat(TestUtils.getPropertyValue(retry, "retryPolicy.maxAttempts"))
				.isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.initialInterval"))
				.isEqualTo(1000L);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.maxInterval"))
				.isEqualTo(10000L);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.multiplier"))
				.isEqualTo(2.0);
		consumerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();

		properties = createConsumerProperties();
		properties.getExtension().setAcknowledgeMode(AcknowledgeMode.NONE);
		properties.setBackOffInitialInterval(2000);
		properties.setBackOffMaxInterval(20000);
		properties.setBackOffMultiplier(5.0);
		properties.setConcurrency(2);
		properties.setMaxAttempts(23);
		properties.getExtension().setMaxConcurrency(3);
		properties.getExtension().setPrefix("foo.");
		properties.getExtension().setPrefetch(20);
		properties.getExtension().setHeaderPatterns(new String[] { "foo" });
		properties.getExtension().setTxSize(10);
		QuorumConfig quorum = properties.getExtension().getQuorum();
		quorum.setEnabled(true);
		quorum.setDeliveryLimit(10);
		quorum.setInitialGroupSize(1);
		properties.setInstanceIndex(0);
		consumerBinding = binder.bindConsumer("props.0", "test",
				createBindableChannel("input", new BindingProperties()), properties);

		endpoint = extractEndpoint(consumerBinding);
		container = verifyContainer(endpoint);


		assertThat(container.getQueueNames()[0]).isEqualTo("foo.props.0.test");

		consumerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void multiplexOnPartitionedConsumer() throws Exception {
		final ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		RabbitTestSupport.RabbitProxy proxy = new RabbitTestSupport.RabbitProxy();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost",
				proxy.getPort());

		final RabbitExchangeQueueProvisioner rabbitExchangeQueueProvisioner = new RabbitExchangeQueueProvisioner(cf);

		consumerProperties.setMultiplex(true);
		consumerProperties.setPartitioned(true);
		consumerProperties.setInstanceIndexList(Arrays.asList(1, 2, 3));

		final ConsumerDestination consumerDestination = rabbitExchangeQueueProvisioner.provisionConsumerDestination("foo", "boo", consumerProperties);

		final String name = consumerDestination.getName();

		assertThat(name).isEqualTo("foo.boo-1,foo.boo-2,foo.boo-3");
	}

	@Test
	void multiplexOnPartitionedConsumerWithMultipleDestinations() throws Exception {
		final ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		RabbitTestSupport.RabbitProxy proxy = new RabbitTestSupport.RabbitProxy();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost",
				proxy.getPort());

		final RabbitExchangeQueueProvisioner rabbitExchangeQueueProvisioner = new RabbitExchangeQueueProvisioner(cf);

		consumerProperties.setMultiplex(true);
		consumerProperties.setPartitioned(true);
		consumerProperties.setInstanceIndexList(Arrays.asList(1, 2, 3));

		final ConsumerDestination consumerDestination = rabbitExchangeQueueProvisioner.provisionConsumerDestination("foo,qaa", "boo", consumerProperties);

		final String name = consumerDestination.getName();

		assertThat(name).isEqualTo("foo.boo-1,foo.boo-2,foo.boo-3,qaa.boo-1,qaa.boo-2,qaa.boo-3");
	}

	@Test
	void consumerPropertiesWithUserInfrastructureNoBind() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());
		Queue queue = new Queue("propsUser1.infra");
		admin.declareQueue(queue);
		DirectExchange exchange = new DirectExchange("propsUser1");
		admin.declareExchange(exchange);
		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("foo"));

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setDeclareExchange(false);
		properties.getExtension().setBindQueue(false);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser1",
				"infra", createBindableChannel("input", new BindingProperties()),
				properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(container, "missingQueuesFatal",
				Boolean.class)).isFalse();
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		List<?> bindings = getBindingsBySource("/", exchange.getName());
		assertThat(bindings.size()).isEqualTo(1);
	}

	@Test
	void anonWithBuiltInExchange() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setDeclareExchange(false);
		properties.getExtension().setQueueNameGroupOnly(true);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("amq.topic", null,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		String queueName = container.getQueueNames()[0];
		assertThat(queueName).startsWith("anonymous.");
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void anonWithBuiltInExchangeCustomPrefix() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setDeclareExchange(false);
		properties.getExtension().setQueueNameGroupOnly(true);
		properties.getExtension().setAnonymousGroupPrefix("customPrefix.");

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("amq.topic", null,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		String queueName = container.getQueueNames()[0];
		assertThat(queueName).startsWith("customPrefix.");
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void consumerPropertiesWithUserInfrastructureCustomExchangeAndRK()
			throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setExchangeType(ExchangeTypes.DIRECT);
		properties.getExtension().setBindingRoutingKey("foo,bar");
		properties.getExtension().setBindingRoutingKeyDelimiter(",");
		properties.getExtension().setQueueNameGroupOnly(true);
		// properties.getExtension().setDelayedExchange(true); // requires delayed message
		// exchange plugin; tested locally

		String group = "infra";
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser2", group,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		ApplicationContext ctx =
				TestUtils.getPropertyValue(binder, "binder.provisioningProvider.autoDeclareContext",
						ApplicationContext.class);
		assertThat(ctx.getBean("infra.binding", Declarables.class).getDeclarables()).hasSize(2);
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		assertThat(container.getQueueNames()[0]).isEqualTo(group);
		List<Map<String, Object>> bindings = getBindingsBySource("/", "propsUser2");
		int n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 2) {
			Thread.sleep(100);
			bindings = getBindingsBySource("/", "propsUser2");
		}
		assertThat(bindings.size()).isEqualTo(2);
		assertThat(bindings.get(0).get("source")).isEqualTo("propsUser2");
		assertThat(bindings.get(0).get("destination")).isEqualTo(group);
		assertThat(bindings.get(0).get("routing_key")).isIn("foo", "bar");
		assertThat(bindings.get(1).get("source")).isEqualTo("propsUser2");
		assertThat(bindings.get(1).get("destination")).isEqualTo(group);
		assertThat(bindings.get(1).get("routing_key")).isIn("foo", "bar");
		assertThat(bindings.get(1).get("routing_key")).isNotEqualTo(bindings.get(0).get("routing_key"));

		Map<String, Object> exchange = getExchange("/", "propsUser2");
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = getExchange("/", "propsUser2");
		}
		assertThat(exchange.get("type")).isEqualTo("direct");
		assertThat(exchange.get("durable")).isEqualTo(true);
		assertThat(exchange.get("auto_delete")).isEqualTo(false);

		verifyAutoDeclareContextClear(binder);
	}

	private String adminUri() {
		return String.format("http://guest:guest@localhost:%d/api", RABBITMQ.getHttpPort());
	}

	@Test
	void consumerPropertiesWithUserInfrastructureCustomQueueArgs()
			throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		RabbitConsumerProperties extProps = properties.getExtension();
		extProps.setExchangeType(ExchangeTypes.DIRECT);
		extProps.setExchangeDurable(false);
		extProps.setExchangeAutoDelete(true);
		extProps.setBindingRoutingKey("foo");
		extProps.setExpires(30_000);
		extProps.setLazy(true);
		extProps.setMaxLength(10_000);
		extProps.setMaxLengthBytes(100_000);
		extProps.setMaxPriority(10);
		extProps.setOverflowBehavior("drop-head");
		extProps.setTtl(2_000);
		extProps.setAutoBindDlq(true);
		extProps.setDeadLetterQueueName("customDLQ");
		extProps.setDeadLetterExchange("customDLX");
		extProps.setDeadLetterExchangeType(ExchangeTypes.TOPIC);
		extProps.setDeadLetterRoutingKey("customDLRK");
		extProps.setDlqDeadLetterExchange("propsUser3");
		// GH-259 - if the next line was commented, the test failed.
		extProps.setDlqDeadLetterRoutingKey("propsUser3");
		extProps.setDlqExpires(60_000);
		extProps.setDlqLazy(true);
		extProps.setDlqMaxLength(20_000);
		extProps.setDlqMaxLengthBytes(40_000);
		extProps.setDlqOverflowBehavior("reject-publish");
		extProps.setDlqMaxPriority(8);
		extProps.setDlqTtl(1_000);
		extProps.setConsumerTagPrefix("testConsumerTag");
		extProps.setExclusive(true);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser3",
				"infra", createBindableChannel("input", new BindingProperties()),
				properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		List<Map<String, Object>> bindings = getBindingsBySource("/", "propsUser3");
		int n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = getBindingsBySource("/", "propsUser3");
		}
		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).get("source")).isEqualTo("propsUser3");
		assertThat(bindings.get(0).get("destination")).isEqualTo("propsUser3.infra");
		assertThat(bindings.get(0).get("routing_key")).isEqualTo("foo");

		bindings = getBindingsBySource("/", "customDLX");
		n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = getBindingsBySource("/", "customDLX");
		}
//		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).get("source")).isEqualTo("customDLX");
		assertThat(bindings.get(0).get("destination")).isEqualTo("customDLQ");
		assertThat(bindings.get(0).get("routing_key")).isEqualTo("customDLRK");

		Map<String, Object> exchange = getExchange("/", "propsUser3");
		n = 0;
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = getExchange("/", "propsUser3");
		}
		assertThat(exchange.get("type")).isEqualTo("direct");
		assertThat(exchange.get("durable")).isEqualTo(false);
		assertThat(exchange.get("auto_delete")).isEqualTo(true);

		exchange = getExchange("/", "customDLX");
		n = 0;
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = getExchange("/", "customDLX");
		}
		assertThat(exchange.get("type")).isEqualTo("topic");
		assertThat(exchange.get("durable")).isEqualTo(true);
		assertThat(exchange.get("auto_delete")).isEqualTo(false);

		Map<String, Object> queue = getQueue("/", "propsUser3.infra");
		n = 0;
		while (n++ < 100 && queue == null || !requiredConsumerState(1, queue)) {
			Thread.sleep(100);
			queue = getQueue("/", "propsUser3.infra");
		}
		assertThat(queue).isNotNull();
		Map<String, Object> args = (Map<String, Object>) queue.get("arguments");
		assertThat(args.get("x-expires")).isEqualTo(30_000);
		assertThat(args.get("x-max-length")).isEqualTo(10_000);
		assertThat(args.get("x-max-length-bytes")).isEqualTo(100_000);
		assertThat(args.get("x-overflow")).isEqualTo("drop-head");
		assertThat(args.get("x-max-priority")).isEqualTo(10);
		assertThat(args.get("x-message-ttl")).isEqualTo(2_000);
		assertThat(args.get("x-dead-letter-exchange")).isEqualTo("customDLX");
		assertThat(args.get("x-dead-letter-routing-key")).isEqualTo("customDLRK");
		assertThat(args.get("x-queue-mode")).isEqualTo("lazy");
		assertThat(queue.get("exclusive_consumer_tag")).isEqualTo("testConsumerTag#0");

		queue = getQueue("/", "customDLQ");

		n = 0;
		while (n++ < 100 && queue == null) {
			Thread.sleep(100);
			queue = getQueue("/", "customDLQ");
		}
		assertThat(queue).isNotNull();
		args = (Map<String, Object>) queue.get("arguments");
		assertThat(args.get("x-expires")).isEqualTo(60_000);
		assertThat(args.get("x-max-length")).isEqualTo(20_000);
		assertThat(args.get("x-max-length-bytes")).isEqualTo(40_000);
		assertThat(args.get("x-overflow")).isEqualTo("reject-publish");
		assertThat(args.get("x-max-priority")).isEqualTo(8);
		assertThat(args.get("x-message-ttl")).isEqualTo(1_000);
		assertThat(args.get("x-dead-letter-exchange")).isEqualTo("propsUser3");
		assertThat(args.get("x-dead-letter-routing-key")).isEqualTo("propsUser3");
		assertThat(args.get("x-queue-mode")).isEqualTo("lazy");

		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void consumerPropertiesWithHeaderExchanges() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setExchangeType(ExchangeTypes.HEADERS);
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setDeadLetterExchange(ExchangeTypes.HEADERS);
		properties.getExtension().setDeadLetterExchange("propsHeader.dlx");
		Map<String, String> queueBindingArguments = new HashMap<>();
		queueBindingArguments.put("x-match", "any");
		queueBindingArguments.put("foo", "bar");
		properties.getExtension().setQueueBindingArguments(queueBindingArguments);
		properties.getExtension().setDlqBindingArguments(queueBindingArguments);

		String group = "bindingArgs";
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsHeader", group,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint,
				"messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		assertThat(container.getQueueNames()[0]).isEqualTo("propsHeader." + group);
		List<Map<String, Object>> bindings = getBindingsBySource("/", "propsHeader");
		int n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = getBindingsBySource("/", "propsHeader");
		}
		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).get("source")).isEqualTo("propsHeader");
		assertThat(bindings.get(0).get("destination")).isEqualTo("propsHeader." + group);
		Map<String, Object> args = (Map<String, Object>) bindings.get(0).get("arguments");
		assertThat(args).hasEntrySatisfying("x-match", v -> assertThat(v).isEqualTo("any"));
		assertThat(args).hasEntrySatisfying("foo", v -> assertThat(v).isEqualTo("bar"));

		bindings = getBindingsBySource("/", "propsHeader.dlx");
		n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = getBindingsBySource("/", "propsHeader.dlx");
		}
		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).get("source")).isEqualTo("propsHeader.dlx");
		assertThat(bindings.get(0).get("destination")).isEqualTo("propsHeader." + group + ".dlq");
		args = (Map<String, Object>) bindings.get(0).get("arguments");
		assertThat(args).hasEntrySatisfying("x-match", v -> assertThat(v).isEqualTo("any"));
		assertThat(args).hasEntrySatisfying("foo", v -> assertThat(v).isEqualTo("bar"));

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void producerProperties(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		Binding<MessageChannel> producerBinding = binder.bindProducer("props.0",
				createBindableChannel("input", new BindingProperties()),
				createProducerProperties(testInfo));
		Lifecycle endpoint = extractEndpoint(producerBinding);
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint,
				"defaultDeliveryMode", MessageDeliveryMode.class);
		assertThat(mode).isEqualTo(MessageDeliveryMode.PERSISTENT);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.matchers", List.class);
		assertThat(requestHeaders).hasSize(5);
		producerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(endpoint, "amqpTemplate.transactional",
				Boolean.class)).isFalse();

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		this.applicationContext.registerBean("pkExtractor",
				TestPartitionKeyExtractorClass.class, () -> new TestPartitionKeyExtractorClass());
		this.applicationContext.registerBean("pkSelector",
				TestPartitionSelectorClass.class, () -> new TestPartitionSelectorClass());
		producerProperties.setPartitionKeyExtractorName("pkExtractor");
		producerProperties.setPartitionSelectorName("pkSelector");
		producerProperties.getExtension().setPrefix("foo.");
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setHeaderPatterns(new String[] { "foo" });
		producerProperties
				.setPartitionKeyExpression(spelExpressionParser.parseExpression("'foo'"));
		producerProperties.setPartitionSelectorExpression(
				spelExpressionParser.parseExpression("0"));
		producerProperties.setPartitionCount(1);
		producerProperties.getExtension().setTransacted(true);
		producerProperties.getExtension()
				.setDelayExpression(spelExpressionParser.parseExpression("42"));
		producerProperties.setRequiredGroups("prodPropsRequired");

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				producerProperties);
		DirectChannel channel = createBindableChannel("output",
				producerBindingProperties);
		producerBinding = binder.bindProducer("props.0", channel, producerProperties);

		ConnectionFactory producerConnectionFactory = TestUtils.getPropertyValue(
				producerBinding, "lifecycle.amqpTemplate.connectionFactory",
				ConnectionFactory.class);

		assertThat(this.rabbitTestSupport.getResource())
				.isSameAs(producerConnectionFactory);

		endpoint = extractEndpoint(producerBinding);
		assertThat(getEndpointRouting(endpoint)).isEqualTo(
				"'props.0-' + headers['" + BinderHeaders.PARTITION_HEADER + "']");
		assertThat(TestUtils
				.getPropertyValue(endpoint, "delayExpression", SpelExpression.class)
				.getExpressionString()).isEqualTo("42");
		mode = TestUtils.getPropertyValue(endpoint, "defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertThat(mode).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(TestUtils.getPropertyValue(endpoint, "amqpTemplate.transactional",
				Boolean.class)).isTrue();
		verifyFooRequestProducer(endpoint);
		channel.send(new GenericMessage<>("foo"));
		org.springframework.amqp.core.Message received = new RabbitTemplate(
				this.rabbitTestSupport.getResource())
						.receive("foo.props.0.prodPropsRequired-0", 10_000);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedDelayLong()).isEqualTo(42);

		producerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void durablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());

		RabbitTestBinder binder = getBinder();

		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setDurableSubscription(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("durableTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("durabletest.0",
				"tgroup", moduleInputChannel, consumerProperties);

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend(TEST_PREFIX + "durabletest.0", "", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template
					.receiveAndConvert(TEST_PREFIX + "durabletest.0.tgroup.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("foo");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		consumerBinding.unbind();
		assertThat(admin.getQueueProperties(TEST_PREFIX + "durabletest.0.tgroup.dlq"))
				.isNotNull();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void nonDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setDurableSubscription(false);
		consumerProperties.setMaxAttempts(1); // disable retry
		BindingProperties bindingProperties = createConsumerBindingProperties(
				consumerProperties);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				bindingProperties);
		moduleInputChannel.setBeanName("nondurabletest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("nondurabletest.0",
				"tgroup", moduleInputChannel, consumerProperties);

		consumerBinding.unbind();
		assertThat(admin.getQueueProperties(TEST_PREFIX + "nondurabletest.0.dlq"))
				.isNull();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void autoBindDLQ() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		BindingProperties bindingProperties = createConsumerBindingProperties(
				consumerProperties);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				bindingProperties);
		moduleInputChannel.setBeanName("dlqTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		consumerProperties.setMultiplex(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("dlqtest,dlqtest2",
				"default", moduleInputChannel, consumerProperties);
		AbstractMessageListenerContainer container = TestUtils.getPropertyValue(
				consumerBinding, "lifecycle.messageListenerContainer",
				AbstractMessageListenerContainer.class);
		assertThat(container.getQueueNames().length).isEqualTo(2);

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqtest.default", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template
					.receiveAndConvert(TEST_PREFIX + "dlqtest.default.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("foo");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		template.convertAndSend("", TEST_PREFIX + "dlqtest2.default", "bar");

		n = 0;
		while (n++ < 100) {
			Object deadLetter = template
					.receiveAndConvert(TEST_PREFIX + "dlqtest2.default.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("bar");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		consumerBinding.unbind();

		ApplicationContext context = TestUtils.getPropertyValue(binder,
				"binder.provisioningProvider.autoDeclareContext",
				ApplicationContext.class);
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.binding"))
				.isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default")).isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq.binding"))
				.isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq")).isFalse();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void autoBindDLQManualAcks() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(2);
		consumerProperties.getExtension().setDurableSubscription(true);
		consumerProperties.getExtension().setAcknowledgeMode(AcknowledgeMode.MANUAL);
		BindingProperties bindingProperties = createConsumerBindingProperties(
				consumerProperties);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				bindingProperties);
		moduleInputChannel.setBeanName("dlqTestManual");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				// Wait until the unacked state is reflected in the admin
				Map<String, Object> info = getQueue("/", TEST_PREFIX + "dlqTestManual.default");
				int n = 0;
				while (n++ < 100 && ( info.get("messages_unacknowledged") == null
						|| ((Long) info.get("messages_unacknowledged")) < 1L)) {
					try {
						Thread.sleep(100);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					info = getQueue("/", TEST_PREFIX + "dlqTestManual.default");
				}
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("dlqTestManual",
				"default", moduleInputChannel, consumerProperties);

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqTestManual.default", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template
					.receiveAndConvert(TEST_PREFIX + "dlqTestManual.default.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("foo");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		n = 0;
		Map<String, Object> info = getQueue("/", TEST_PREFIX + "dlqTestManual.default");
		while (n++ < 100 && unackedMessages(info)) {
			Thread.sleep(100);
			info = getQueue("/", TEST_PREFIX + "dlqTestManual.default");
		}
		assertThat(unackedMessages(info)).isFalse();

		consumerBinding.unbind();

		ApplicationContext context = TestUtils.getPropertyValue(binder,
				"binder.provisioningProvider.autoDeclareContext",
				ApplicationContext.class);
		assertThat(context.containsBean(TEST_PREFIX + "dlqTestManual.default.binding"))
				.isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqTestManual.default")).isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqTestManual.default.dlq.binding"))
				.isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqTestManual.default.dlq")).isFalse();

		verifyAutoDeclareContextClear(binder);
	}

	protected boolean unackedMessages(Map<String, Object> info) {
		Object unack = info.get("messages_unacknowledged");
		return unack != null && ((Integer) unack) > 0L;
	}


	@Test
	void autoBindDLQPartionedConsumerFirst(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.setMaxAttempts(1); // disable retry
		properties.setPartitioned(true);
		properties.setInstanceIndex(0);
		DirectChannel input0 = createBindableChannel("input",
				createConsumerBindingProperties(properties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.0",
				"dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.0",
				"default", new QueueChannel(), properties);
		properties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1",
				createConsumerBindingProperties(properties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.0",
				"dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.0",
				"default", new QueueChannel(), properties);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setPrefix("bindertest.");
		this.applicationContext.registerBean("pkExtractor", PartitionTestSupport.class, () -> new PartitionTestSupport());
		this.applicationContext.registerBean("pkSelector", PartitionTestSupport.class, () -> new PartitionTestSupport());
		producerProperties.getExtension().setAutoBindDlq(true);
		producerProperties.setPartitionKeyExtractorName("pkExtractor");
		producerProperties.setPartitionSelectorName("pkSelector");
		producerProperties.setPartitionCount(2);
		BindingProperties bindingProperties = createProducerBindingProperties(
				producerProperties);
		DirectChannel output = createBindableChannel("output", bindingProperties);
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.0", output,
				producerProperties);

		final CountDownLatch latch0 = new CountDownLatch(1);
		input0.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch0.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch0.countDown();
			}

		});

		final CountDownLatch latch1 = new CountDownLatch(1);
		input1.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch1.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch1.countDown();
			}

		});

		output.send(new GenericMessage<>(1));
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<>(0));
		assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<>(1));

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.0.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.0.dlqPartGrp-1");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		output.send(new GenericMessage<>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.0.dlqPartGrp-0");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	@Disabled
	void autoBindDLQPartitionedConsumerFirstWithRepublishNoRetry(TestInfo testInfo)
			throws Exception {
		testAutoBindDLQPartionedConsumerFirstWithRepublishGuts(false, testInfo);
	}

	@Test
	void autoBindDLQPartitionedConsumerFirstWithRepublishWithRetry(TestInfo testInfo)
			throws Exception {
		testAutoBindDLQPartionedConsumerFirstWithRepublishGuts(true, testInfo);
	}

	private void testAutoBindDLQPartionedConsumerFirstWithRepublishGuts(
			final boolean withRetry, TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setRepublishToDlq(true);
		properties.getExtension()
				.setRepublishDeliveyMode(MessageDeliveryMode.NON_PERSISTENT);
		properties.setMaxAttempts(withRetry ? 2 : 1);
		properties.setPartitioned(true);
		properties.setInstanceIndex(0);
		properties.populateBindingName("blahblah");
		DirectChannel input0 = createBindableChannel("input",
				createConsumerBindingProperties(properties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partPubDLQ.0",
				"dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder
				.bindConsumer("partPubDLQ.0", "default", new QueueChannel(), properties);
		properties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1",
				createConsumerBindingProperties(properties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partPubDLQ.0",
				"dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder
				.bindConsumer("partPubDLQ.0", "default", new QueueChannel(), properties);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setPrefix("bindertest.");
		producerProperties.getExtension().setAutoBindDlq(true);
		this.applicationContext.registerBean("pkExtractor", PartitionTestSupport.class, () -> new PartitionTestSupport());
		this.applicationContext.registerBean("pkSelector", PartitionTestSupport.class, () -> new PartitionTestSupport());
		producerProperties.setPartitionKeyExtractorName("pkExtractor");
		producerProperties.setPartitionSelectorName("pkSelector");
		producerProperties.setPartitionCount(2);
		BindingProperties bindingProperties = createProducerBindingProperties(
				producerProperties);
		DirectChannel output = createBindableChannel("output", bindingProperties);
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partPubDLQ.0",
				output, producerProperties);

		final CountDownLatch latch0 = new CountDownLatch(1);
		input0.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch0.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch0.countDown();
			}

		});

		final CountDownLatch latch1 = new CountDownLatch(1);
		input1.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch1.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch1.countDown();
			}

		});
		ApplicationContext context = TestUtils.getPropertyValue(binder.getBinder(),
				"applicationContext", ApplicationContext.class);
		SubscribableChannel boundErrorChannel = context.getBean(testBinder.getBinder().getBinderIdentity() + ".blahblah.errors", SubscribableChannel.class);
		SubscribableChannel globalErrorChannel = context.getBean("errorChannel",
				SubscribableChannel.class);
		final AtomicReference<Message<?>> boundErrorChannelMessage = new AtomicReference<>();
		final AtomicReference<Message<?>> globalErrorChannelMessage = new AtomicReference<>();
		final AtomicBoolean hasRecovererInCallStack = new AtomicBoolean(!withRetry);
		boundErrorChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				boundErrorChannelMessage.set(message);
				String stackTrace = Arrays
						.toString(new RuntimeException().getStackTrace());
				hasRecovererInCallStack
						.set(stackTrace.contains("ErrorMessageSendingRecoverer"));
			}

		});
		globalErrorChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				globalErrorChannelMessage.set(message);
			}

		});

		output.send(new GenericMessage<>(1));
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<>(0));
		assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<>(1));

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partPubDLQ.0.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(
				received.getMessageProperties().getHeaders().get("x-original-routingKey"))
						.isEqualTo("partPubDLQ.0-1");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);
		assertThat(received.getMessageProperties().getReceivedDeliveryMode())
				.isEqualTo(MessageDeliveryMode.NON_PERSISTENT);

		output.send(new GenericMessage<>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(
				received.getMessageProperties().getHeaders().get("x-original-routingKey"))
						.isEqualTo("partPubDLQ.0-0");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		// verify we got a message on the dedicated error channel and the global (via
		// bridge)
	//	assertThat(boundErrorChannelMessage.get()).isNotNull();
	//	assertThat(globalErrorChannelMessage.get()).isNotNull();
//		assertThat(hasRecovererInCallStack.get()).isEqualTo(withRetry);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void autoBindDLQPartitionedProducerFirst(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> properties = createProducerProperties(testInfo);

		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.setRequiredGroups("dlqPartGrp");
		this.applicationContext.registerBean("pkExtractor", PartitionTestSupport.class, () -> new PartitionTestSupport());
		properties.setPartitionKeyExtractorName("pkExtractor");
		properties.setPartitionSelectorName("pkExtractor");
		properties.setPartitionCount(2);
		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(properties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.1", output,
				properties);

		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix("bindertest.");
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.setPartitioned(true);
		consumerProperties.setInstanceIndex(0);
		DirectChannel input0 = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.1",
				"dlqPartGrp", input0, consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.1",
				"defaultConsumer", new QueueChannel(), consumerProperties);
		consumerProperties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1",
				createConsumerBindingProperties(consumerProperties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.1",
				"dlqPartGrp", input1, consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.1",
				"defaultConsumer", new QueueChannel(), consumerProperties);

		final CountDownLatch latch0 = new CountDownLatch(1);
		input0.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch0.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch0.countDown();
			}

		});

		final CountDownLatch latch1 = new CountDownLatch(1);
		input1.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (latch1.getCount() <= 0) {
					throw new RuntimeException("dlq");
				}
				latch1.countDown();
			}

		});

		output.send(new GenericMessage<Integer>(1));
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<Integer>(0));
		assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();

		output.send(new GenericMessage<Integer>(1));

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.1.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.1.dlqPartGrp-1");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);
		assertThat(received.getMessageProperties().getReceivedDeliveryMode())
				.isEqualTo(MessageDeliveryMode.PERSISTENT);

		output.send(new GenericMessage<Integer>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.1.dlqPartGrp-0");
		assertThat(received.getMessageProperties().getHeaders())
				.doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void autoBindDLQwithRepublish() throws Exception {
		this.maxStackTraceSize = RabbitUtils
				.getMaxFrame(rabbitTestSupport.getResource()) - 20_000;
		assertThat(this.maxStackTraceSize).isGreaterThan(0);

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("dlqPubTest");
		RuntimeException exception = bigCause(
				new RuntimeException(BIG_EXCEPTION_MESSAGE));
		assertThat(getStackTraceAsString(exception).length())
				.isGreaterThan(this.maxStackTraceSize);
		AtomicBoolean dontRepublish = new AtomicBoolean();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				if (dontRepublish.get()) {
					throw new ImmediateAcknowledgeAmqpException("testDontRepublish");
				}
				throw exception;
			}

		});
		consumerProperties.setMultiplex(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"foo.dlqpubtest,foo.dlqpubtest2", "foo", moduleInputChannel,
				consumerProperties);

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtest.foo", "foo");

		template.setReceiveTimeout(10_000);
		org.springframework.amqp.core.Message deadLetter = template
				.receive(TEST_PREFIX + "foo.dlqpubtest.foo.dlq");
		assertThat(deadLetter).isNotNull();
		assertThat(new String(deadLetter.getBody())).isEqualTo("foo");
		assertThat(deadLetter.getMessageProperties().getHeaders())
				.containsKey((RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE));
		assertThat(((LongString) deadLetter.getMessageProperties().getHeaders()
				.get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE)).length())
						.isEqualTo(this.maxStackTraceSize);

		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtest2.foo", "bar");

		deadLetter = template.receive(TEST_PREFIX + "foo.dlqpubtest2.foo.dlq");
		assertThat(deadLetter).isNotNull();
		assertThat(new String(deadLetter.getBody())).isEqualTo("bar");
		assertThat(deadLetter.getMessageProperties().getHeaders())
				.containsKey(("x-exception-stacktrace"));

		dontRepublish.set(true);
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtest2.foo", "baz");
		template.setReceiveTimeout(500);
		assertThat(template.receive(TEST_PREFIX + "foo.dlqpubtest2.foo.dlq")).isNull();

		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void autoBindDLQwithRepublishTx() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		consumerProperties.getExtension().setTransacted(true);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("dlqPubTestTx");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("test");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"foo.dlqpubtestTx", "foo", moduleInputChannel, consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtestTx.foo", "foo");

		template.setReceiveTimeout(10_000);
		org.springframework.amqp.core.Message deadLetter = template
				.receive(TEST_PREFIX + "foo.dlqpubtestTx.foo.dlq");
		assertThat(deadLetter).isNotNull();
		assertThat(deadLetter.getBody()).isEqualTo("foo".getBytes());
		assertThat(deadLetter.getMessageProperties().getHeaders())
				.containsKey((RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE));
		List<Object> errorHandler = (List<Object>) TestUtils.getPropertyValue(consumerBinding,
				"lifecycle.errorChannel.dispatcher.handlers", Set.class).stream()
			.filter(handler -> !handler.getClass().equals(BridgeHandler.class))
			.collect(Collectors.toList());
		assertThat(errorHandler).hasSize(1);
		assertThat(TestUtils.getPropertyValue(errorHandler.get(0), "template.transactional", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(errorHandler.get(0), "confirmType", ConfirmType.class))
				.isEqualTo(ConfirmType.NONE);
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void autoBindDLQwithRepublishSimpleConfirms() throws Exception {
		CachingConnectionFactory ccf = this.rabbitTestSupport.getResource();
		ccf.setPublisherReturns(true);
		ccf.setPublisherConfirmType(ConfirmType.SIMPLE);
		ccf.resetConnection();
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("dlqPubtestSimple");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("test");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"foo.dlqpubtestSimple", "foo", moduleInputChannel, consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtestSimple.foo", "foo");

		template.setReceiveTimeout(10_000);
		org.springframework.amqp.core.Message deadLetter = template
				.receive(TEST_PREFIX + "foo.dlqpubtestSimple.foo.dlq");
		assertThat(deadLetter).isNotNull();
		assertThat(deadLetter.getBody()).isEqualTo("foo".getBytes());
		assertThat(deadLetter.getMessageProperties().getHeaders())
				.containsKey((RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE));
		List<Object> errorHandler = (List<Object>) TestUtils.getPropertyValue(consumerBinding,
				"lifecycle.errorChannel.dispatcher.handlers", Set.class).stream()
			.filter(handler -> !handler.getClass().equals(BridgeHandler.class))
			.collect(Collectors.toList());
		assertThat(errorHandler).hasSize(1);
		assertThat(TestUtils.getPropertyValue(errorHandler.get(0), "confirmType", ConfirmType.class))
				.isEqualTo(ConfirmType.SIMPLE);
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void autoBindDLQwithRepublishCorrelatedConfirms() throws Exception {
		CachingConnectionFactory ccf = this.rabbitTestSupport.getResource();
		ccf.setPublisherReturns(true);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.resetConnection();
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("dlqPubtestCorrelated");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("test");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"foo.dlqpubtestCorrelated", "foo", moduleInputChannel, consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitTestSupport.getResource());
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtestCorrelated.foo", "foo");

		template.setReceiveTimeout(10_000);
		org.springframework.amqp.core.Message deadLetter = template
				.receive(TEST_PREFIX + "foo.dlqpubtestCorrelated.foo.dlq");
		assertThat(deadLetter).isNotNull();
		assertThat(deadLetter.getBody()).isEqualTo("foo".getBytes());
		assertThat(deadLetter.getMessageProperties().getHeaders())
				.containsKey((RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE));
		List<Object> errorHandler = (List<Object>) TestUtils.getPropertyValue(consumerBinding,
				"lifecycle.errorChannel.dispatcher.handlers", Set.class).stream()
			.filter(handler -> !handler.getClass().equals(BridgeHandler.class))
			.collect(Collectors.toList());
		assertThat(errorHandler).hasSize(1);
		assertThat(TestUtils.getPropertyValue(errorHandler.get(0), "confirmType", ConfirmType.class))
				.isEqualTo(ConfirmType.CORRELATED);
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void batchingAndCompression(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setBatchingEnabled(true);
		producerProperties.getExtension().setBatchSize(2);
		producerProperties.getExtension().setBatchBufferLimit(100000);
		producerProperties.getExtension().setBatchTimeout(30000);
		producerProperties.getExtension().setCompress(true);
		producerProperties.setRequiredGroups("default");

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("batchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("batching.0",
				output, producerProperties);

		Log logger = spy(TestUtils.getPropertyValue(binder,
				"binder.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(
				TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor"))
						.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertThat(TestUtils.getPropertyValue(binder,
				"binder.compressingPostProcessor.level")).isEqualTo(Deflater.BEST_SPEED);

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Object out = spyOn("batching.0.default").receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out))
				.isEqualTo("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar");

		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).trace(captor.capture());
		assertThat(captor.getValue().toString()).contains(("Compressed 14 to "));

		QueueChannel input = new QueueChannel();
		input.setBeanName("batchingConsumer");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("batching.0",
				"test", input, createConsumerProperties());

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Message<byte[]> in = (Message<byte[]>) input.receive(10000);
		assertThat(in).isNotNull();
		assertThat(new String(in.getPayload())).isEqualTo("foo");
		in = (Message<byte[]>) input.receive(10000);
		assertThat(in).isNotNull();
		assertThat(new String(in.getPayload())).isEqualTo("bar");
		assertThat(in.getHeaders().get(AmqpHeaders.DELIVERY_MODE)).isNull();

		producerBinding.unbind();
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void producerBatching(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setBatchingEnabled(true);
		producerProperties.getExtension().setBatchSize(2);
		producerProperties.getExtension().setBatchBufferLimit(100000);
		producerProperties.getExtension().setBatchTimeout(30000);
		producerProperties.getExtension().setCompress(true);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("producerBatchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("p.batching.0",
				output, producerProperties);

		QueueChannel input = new QueueChannel();
		input.setBeanName("producerBatchingConsumer");
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(true);
		consumerProperties.getExtension().setBatchSize(2);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("p.batching.0",
				"producerBatching", input, consumerProperties);
		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));


		Message<?> in = input.receive(10000);
		assertThat(in).isNotNull();
		assertThat(in.getPayload()).isInstanceOf(List.class);
		List<byte[]> payload = (List<byte[]>) in.getPayload();
		assertThat(payload).hasSize(2);
		assertThat(payload.get(0)).isEqualTo("foo".getBytes());
		assertThat(payload.get(1)).isEqualTo("bar".getBytes());

		producerBinding.unbind();
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@SuppressWarnings("unchecked")
	@Test
	void consumerBatching(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("consumerBatching.Producer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("c.batching.0",
				output, producerProperties);

		QueueChannel input = new QueueChannel();
		input.setBeanName("batchingConsumer");
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(true);
		consumerProperties.getExtension().setBatchSize(2);
		consumerProperties.getExtension().setEnableBatching(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("c.batching.0",
				"consumerBatching", input, consumerProperties);
		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));


		Message<?> in = input.receive(10000);
		assertThat(in).isNotNull();
		assertThat(in.getPayload()).isInstanceOf(List.class);
		List<byte[]> payload = (List<byte[]>) in.getPayload();
		assertThat(payload).hasSize(2);
		assertThat(payload.get(0)).isEqualTo("foo".getBytes());
		assertThat(payload.get(1)).isEqualTo("bar".getBytes());

		producerBinding.unbind();
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void errorMessageHandlerForBatchModeDLQRepublish(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("consumerBatching.Producer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("c.batching.0",
				output, producerProperties);

		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(true);
		consumerProperties.getExtension().setBatchSize(2);
		consumerProperties.getExtension().setEnableBatching(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setDeadLetterExchange("dlqTest");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("c.batching.0",
				"consumerBatching", moduleInputChannel, consumerProperties);

		List<String> expectedMessages = List.of("message1","message2");

		output.send(new GenericMessage<>(expectedMessages.get(0).getBytes()));
		output.send(new GenericMessage<>(expectedMessages.get(1).getBytes()));

		moduleInputChannel.subscribe(message -> {
			throw new RuntimeException("Exception");
		});

		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		long now = Instant.now().toEpochMilli();
		List<org.springframework.amqp.core.Message> deadLetterMessages = new ArrayList<>();
		while(deadLetterMessages.size() < 2 || (Instant.now().toEpochMilli() - now) > 60_000){
			deadLetterMessages.add(template
					.receive("c.batching.0.consumerBatching.dlq", 10_000));
		}

		assertThat(new String(deadLetterMessages.get(0).getBody())).isIn(expectedMessages);
		assertThat(new String(deadLetterMessages.get(0).getBody())).isIn(expectedMessages);

		producerBinding.unbind();
		consumerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@SuppressWarnings("unchecked")
	@Test
	void internalHeadersNotPropagated(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension()
				.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("propagate.out");
		Binding<MessageChannel> producerBinding = binder.bindProducer("propagate.1",
				output, producerProperties);

		QueueChannel input = new QueueChannel();
		input.setBeanName("propagate.in");
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propagate.0",
				"propagate", input, consumerProperties);
		RabbitAdmin admin = new RabbitAdmin(rabbitTestSupport.getResource());
		admin.declareQueue(new Queue("propagate"));
		admin.declareBinding(new org.springframework.amqp.core.Binding("propagate", DestinationType.QUEUE,
				"propagate.1", "#", null));
		RabbitTemplate template = new RabbitTemplate(this.rabbitTestSupport.getResource());
		template.convertAndSend("propagate.0.propagate", "foo");
		output.send(input.receive(10_000));
		org.springframework.amqp.core.Message received = template.receive("propagate", 10_000);
		assertThat(received).isNotNull();
		assertThat(received.getBody()).isEqualTo("foo".getBytes());
		Object header = received.getMessageProperties().getHeader(IntegrationMessageHeaderAccessor.SOURCE_DATA);
		assertThat(header).isNull();
		header = received.getMessageProperties().getHeader(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
		assertThat(header).isNull();

		producerBinding.unbind();
		consumerBinding.unbind();
		admin.deleteQueue("propagate");

		verifyAutoDeclareContextClear(binder);
	}


	/*
	 * Test late binding due to broker down; queues with and without DLQs, and partitioned
	 * queues.
	 */
	@Test
	void lateBinding(TestInfo testInfo) throws Exception {
		RabbitTestSupport.RabbitProxy proxy = new RabbitTestSupport.RabbitProxy();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost",
				proxy.getPort());

		RabbitMessageChannelBinder rabbitBinder = new RabbitMessageChannelBinder(cf,
				new RabbitProperties(), new RabbitExchangeQueueProvisioner(cf));
		RabbitTestBinder binder = new RabbitTestBinder(cf, rabbitBinder);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setPrefix("latebinder.");
		producerProperties.getExtension().setAutoBindDlq(true);
		producerProperties.getExtension().setTransacted(true);

		MessageChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> late0ProducerBinding = binder.bindProducer("late.0",
				moduleOutputChannel, producerProperties);

		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> rabbitConsumerProperties = createConsumerProperties();
		rabbitConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> late0ConsumerBinding = binder.bindConsumer("late.0",
				"test", moduleInputChannel, rabbitConsumerProperties);

		producerProperties.setPartitionKeyExpression(
				spelExpressionParser.parseExpression("payload.equals('0') ? 0 : 1"));
		producerProperties.setPartitionSelectorExpression(
				spelExpressionParser.parseExpression("hashCode()"));
		producerProperties.setPartitionCount(2);

		MessageChannel partOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> partlate0ProducerBinding = binder
				.bindProducer("partlate.0", partOutputChannel, producerProperties);

		QueueChannel partInputChannel0 = new QueueChannel();
		QueueChannel partInputChannel1 = new QueueChannel();

		ExtendedConsumerProperties<RabbitConsumerProperties> partLateConsumerProperties = createConsumerProperties();
		partLateConsumerProperties.getExtension().setPrefix("latebinder.");
		partLateConsumerProperties.setPartitioned(true);
		partLateConsumerProperties.setInstanceIndex(0);
		Binding<MessageChannel> partlate0Consumer0Binding = binder.bindConsumer(
				"partlate.0", "test", partInputChannel0, partLateConsumerProperties);
		partLateConsumerProperties.setInstanceIndex(1);
		Binding<MessageChannel> partlate0Consumer1Binding = binder.bindConsumer(
				"partlate.0", "test", partInputChannel1, partLateConsumerProperties);

		ExtendedProducerProperties<RabbitProducerProperties> noDlqProducerProperties = createProducerProperties(testInfo);
		noDlqProducerProperties.getExtension().setPrefix("latebinder.");
		MessageChannel noDLQOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(noDlqProducerProperties));
		Binding<MessageChannel> noDlqProducerBinding = binder.bindProducer("lateNoDLQ.0",
				noDLQOutputChannel, noDlqProducerProperties);

		QueueChannel noDLQInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> noDlqConsumerProperties = createConsumerProperties();
		noDlqConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> noDlqConsumerBinding = binder.bindConsumer("lateNoDLQ.0",
				"test", noDLQInputChannel, noDlqConsumerProperties);

		MessageChannel outputChannel = createBindableChannel("output",
				createProducerBindingProperties(noDlqProducerProperties));
		Binding<MessageChannel> pubSubProducerBinding = binder.bindProducer("latePubSub",
				outputChannel, noDlqProducerProperties);
		QueueChannel pubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(false);
		Binding<MessageChannel> nonDurableConsumerBinding = binder.bindConsumer(
				"latePubSub", "lategroup", pubSubInputChannel, noDlqConsumerProperties);
		QueueChannel durablePubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(true);
		Binding<MessageChannel> durableConsumerBinding = binder.bindConsumer("latePubSub",
				"lateDurableGroup", durablePubSubInputChannel, noDlqConsumerProperties);

		proxy.start(RABBITMQ.getAmqpPort());

		moduleOutputChannel.send(MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		Message<?> message = moduleInputChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isNotNull();

		noDLQOutputChannel.send(MessageBuilder.withPayload("bar")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		message = noDLQInputChannel.receive(10000);
		assertThat(message);
		assertThat(message.getPayload()).isEqualTo("bar".getBytes());

		outputChannel.send(MessageBuilder.withPayload("baz")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		message = pubSubInputChannel.receive(10000);
		assertThat(message);
		assertThat(message.getPayload()).isEqualTo("baz".getBytes());
		message = durablePubSubInputChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("baz".getBytes());

		partOutputChannel.send(MessageBuilder.withPayload("0")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		partOutputChannel.send(MessageBuilder.withPayload("1")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		message = partInputChannel0.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("0".getBytes());
		message = partInputChannel1.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("1".getBytes());

		late0ProducerBinding.unbind();
		late0ConsumerBinding.unbind();
		partlate0ProducerBinding.unbind();
		partlate0Consumer0Binding.unbind();
		partlate0Consumer1Binding.unbind();
		noDlqProducerBinding.unbind();
		noDlqConsumerBinding.unbind();
		pubSubProducerBinding.unbind();
		nonDurableConsumerBinding.unbind();
		durableConsumerBinding.unbind();

		binder.cleanup();

		proxy.stop();
		cf.destroy();

		this.rabbitTestSupport.getResource().destroy();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void badUserDeclarationsFatal() throws Exception {
		RabbitTestBinder binder = getBinder();
		ConfigurableApplicationContext context = binder.getApplicationContext();
		ConfigurableListableBeanFactory bf = context.getBeanFactory();
		bf.registerSingleton("testBadUserDeclarationsFatal",
				new Queue("testBadUserDeclarationsFatal", false));
		bf.registerSingleton("binder", binder);
		RabbitExchangeQueueProvisioner provisioner = TestUtils.getPropertyValue(binder,
				"binder.provisioningProvider", RabbitExchangeQueueProvisioner.class);
		bf.initializeBean(provisioner, "provisioner");
		bf.registerSingleton("provisioner", provisioner);
		context.addApplicationListener(provisioner);
		RabbitAdmin admin = new RabbitAdmin(rabbitTestSupport.getResource());
		admin.declareQueue(new Queue("testBadUserDeclarationsFatal"));
		// reset the connection and configure the "user" admin to auto declare queues...
		rabbitTestSupport.getResource().resetConnection();
		bf.initializeBean(admin, "rabbitAdmin");
		bf.registerSingleton("rabbitAdmin", admin);
		admin.afterPropertiesSet();
		// the mis-configured queue should be fatal
		Binding<?> binding = null;
		try {
			binding = binder.bindConsumer("input", "baddecls",
					this.createBindableChannel("input", new BindingProperties()),
					createConsumerProperties());
			fail("Expected exception");
		}
		catch (BinderException e) {
			assertThat(e.getCause()).isInstanceOf(AmqpIOException.class);
		}
		finally {
			admin.deleteQueue("testBadUserDeclarationsFatal");
			if (binding != null) {
				binding.unbind();
			}
		}

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void routingKeyExpression(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setRoutingKeyExpression(
				spelExpressionParser.parseExpression("payload.field"));

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("rkeProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("rke", output,
				producerProperties);

		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());
		Queue queue = new AnonymousQueue();
		TopicExchange exchange = new TopicExchange("rke");
		org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue)
				.to(exchange).with("rkeTest");
		admin.declareQueue(queue);
		admin.declareBinding(binding);

		output.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				assertThat(message.getHeaders()
						.get(RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER))
								.isEqualTo("rkeTest");
				return message;
			}

		});

		output.send(new GenericMessage<>(new Pojo("rkeTest")));

		Object out = spyOn(queue.getName()).receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out, StandardCharsets.UTF_8))
				.isEqualTo("{\"field\":\"rkeTest\"}");

		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void routingKey(TestInfo testInfo) throws Exception {
		String routingKey = "static.key";
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setRoutingKey(routingKey);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("rkeProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("rke", output,
				producerProperties);

		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());
		Queue queue = new AnonymousQueue();
		DirectExchange exchange = new DirectExchange("rke");
		org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue)
				.to(exchange).with(routingKey);
		admin.declareQueue(queue);
		admin.declareBinding(binding);

		output.send(new GenericMessage<>(new Pojo("rkeTest")));

		Object out = spyOn(queue.getName()).receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out, StandardCharsets.UTF_8))
				.isEqualTo("{\"field\":\"rkeTest\"}");


		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void routingKeyExpressionPartitionedAndDelay(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setRoutingKeyExpression(
				spelExpressionParser.parseExpression("#root.getPayload().field"));
		// requires delayed message exchange plugin; tested locally
		// producerProperties.getExtension().setDelayedExchange(true);
		producerProperties.getExtension()
				.setDelayExpression(spelExpressionParser.parseExpression("1000"));
		producerProperties.setPartitionKeyExpression(new ValueExpression<>(0));

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("rkeProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("rkep", output,
				producerProperties);

		RabbitAdmin admin = new RabbitAdmin(this.rabbitTestSupport.getResource());
		Queue queue = new AnonymousQueue();
		TopicExchange exchange = new TopicExchange("rkep");
		org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue)
				.to(exchange).with("rkepTest-0");
		admin.declareQueue(queue);
		admin.declareBinding(binding);

		output.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				assertThat(message.getHeaders()
						.get(RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER))
								.isEqualTo("rkepTest");
				assertThat(message.getHeaders()
						.get(RabbitExpressionEvaluatingInterceptor.DELAY_HEADER))
								.isEqualTo(1000);
				return message;
			}

		});

		output.send(new GenericMessage<>(new Pojo("rkepTest")));

		Object out = spyOn(queue.getName()).receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out, StandardCharsets.UTF_8))
				.isEqualTo("{\"field\":\"rkepTest\"}");

		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void polledConsumer() throws Exception {
		RabbitTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(
				this.messageConverter);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer(
				"pollable", "group", inboundBindTarget, createConsumerProperties());
		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("pollable.group", "testPollable");
		boolean polled = inboundBindTarget.poll(m -> {
			assertThat(m.getPayload()).isEqualTo("testPollable");
		});
		int n = 0;
		while (n++ < 100 && !polled) {
			polled = inboundBindTarget.poll(m -> {
				assertThat(m.getPayload()).isEqualTo("testPollable");
			});
		}
		assertThat(polled).isTrue();
		binding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void polledConsumerRequeue() throws Exception {
		RabbitTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(
				this.messageConverter);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer(
				"pollableRequeue", "group", inboundBindTarget, properties);
		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("pollableRequeue.group", "testPollable");
		try {
			boolean polled = false;
			int n = 0;
			while (n++ < 100 && !polled) {
				polled = inboundBindTarget.poll(m -> {
					assertThat(m.getPayload()).isEqualTo("testPollable");
					throw new RequeueCurrentMessageException();
				});
			}
		}
		catch (MessageHandlingException e) {
			assertThat(e.getCause()).isInstanceOf(RequeueCurrentMessageException.class);
		}
		boolean polled = inboundBindTarget.poll(m -> {
			assertThat(m.getPayload()).isEqualTo("testPollable");
		});
		assertThat(polled).isTrue();
		binding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void polledConsumerWithDlq() throws Exception {
		RabbitTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(
				this.messageConverter);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		properties.getExtension().setAutoBindDlq(true);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer(
				"pollableDlq", "group", inboundBindTarget, properties);
		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("pollableDlq.group", "testPollable");
		try {
			int n = 0;
			while (n++ < 100) {
				inboundBindTarget.poll(m -> {
					throw new RuntimeException("test DLQ");
				});
				Thread.sleep(100);
			}
		}
		catch (MessageHandlingException e) {
			assertThat(
					e.getCause().getCause().getCause().getCause().getCause().getMessage())
							.isEqualTo("test DLQ");
		}
		org.springframework.amqp.core.Message deadLetter = template
				.receive("pollableDlq.group.dlq", 10_000);
		assertThat(deadLetter).isNotNull();
		binding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void polledConsumerWithDlqNoRetry() throws Exception {
		RabbitTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(
				this.messageConverter);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.setMaxAttempts(1);
		// properties.getExtension().setRequeueRejected(true); // loops, correctly
		properties.getExtension().setAutoBindDlq(true);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer(
				"pollableDlqNoRetry", "group", inboundBindTarget, properties);
		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("pollableDlqNoRetry.group", "testPollable");
		try {
			int n = 0;
			while (n++ < 100) {
				inboundBindTarget.poll(m -> {
					throw new RuntimeException("test DLQ");
				});
				Thread.sleep(100);
			}
		}
		catch (MessageHandlingException e) {
			assertThat(e.getCause().getMessage()).isEqualTo("test DLQ");
		}
		org.springframework.amqp.core.Message deadLetter = template
				.receive("pollableDlqNoRetry.group.dlq", 10_000);
		assertThat(deadLetter).isNotNull();
		binding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	@Test
	void polledConsumerWithDlqRePub() throws Exception {
		RabbitTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(
				this.messageConverter);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setRepublishToDlq(true);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer(
				"pollableDlqRePub", "group", inboundBindTarget, properties);
		RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.convertAndSend("pollableDlqRePub.group", "testPollable");
		boolean polled = false;
		int n = 0;
		while (n++ < 100 && !polled) {
			Thread.sleep(100);
			polled = inboundBindTarget.poll(m -> {
				throw new RuntimeException("test DLQ");
			});
		}
		assertThat(polled).isTrue();
		org.springframework.amqp.core.Message deadLetter = template
				.receive("pollableDlqRePub.group.dlq", 10_000);
		assertThat(deadLetter).isNotNull();
		binding.unbind();

		verifyAutoDeclareContextClear(binder);
	}

	@Test
	void customBatchingStrategy(TestInfo testInfo) throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setBatchingEnabled(true);
		producerProperties.getExtension().setBatchingStrategyBeanName("testCustomBatchingStrategy");
		producerProperties.setRequiredGroups("default");

		ConfigurableListableBeanFactory beanFactory = binder.getApplicationContext().getBeanFactory();
		beanFactory.registerSingleton("testCustomBatchingStrategy", new TestBatchingStrategy());

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		output.setBeanName("batchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("batching.0", output, producerProperties);

		Log logger = spy(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor"))
				.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertThat(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.level"))
				.isEqualTo(Deflater.BEST_SPEED);

		output.send(new GenericMessage<>("0".getBytes()));
		output.send(new GenericMessage<>("1".getBytes()));
		output.send(new GenericMessage<>("2".getBytes()));
		output.send(new GenericMessage<>("3".getBytes()));
		output.send(new GenericMessage<>("4".getBytes()));

		Object out = spyOn("batching.0.default").receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out)).isEqualTo("0\u0000\n1\u0000\n2\u0000\n3\u0000\n4\u0000\n");

		producerBinding.unbind();

		verifyAutoDeclareContextClear(binder);
	}


	private SimpleMessageListenerContainer verifyContainer(Lifecycle endpoint) {
		SimpleMessageListenerContainer container;
		RetryTemplate retry;
		container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.NONE);
		assertThat(container.getQueueNames()[0]).startsWith("foo.props.0");
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class))
				.isFalse();
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers"))
				.isEqualTo(2);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers"))
				.isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(container, "defaultRequeueRejected",
				Boolean.class)).isFalse();
		assertThat(TestUtils.getPropertyValue(container, "prefetchCount")).isEqualTo(20);
		assertThat(TestUtils.getPropertyValue(container, "batchSize")).isEqualTo(10);
		retry = TestUtils.getPropertyValue(endpoint, "retryTemplate",
				RetryTemplate.class);
		assertThat(TestUtils.getPropertyValue(retry, "retryPolicy.maxAttempts"))
				.isEqualTo(23);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.initialInterval"))
				.isEqualTo(2000L);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.maxInterval"))
				.isEqualTo(20000L);
		assertThat(TestUtils.getPropertyValue(retry, "backOffPolicy.multiplier"))
				.isEqualTo(5.0);

		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.matchers", List.class);
		assertThat(requestMatchers).hasSize(1);
		assertThat(TestUtils.getPropertyValue(requestMatchers.get(0), "pattern"))
				.isEqualTo("foo");

		return container;
	}

	private void verifyFooRequestProducer(Lifecycle endpoint) {
		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.matchers", List.class);
		assertThat(requestMatchers).hasSize(5);
		assertThat(TestUtils.getPropertyValue(requestMatchers.get(4), "pattern"))
				.isEqualTo("foo");
	}

	@Override
	protected String getEndpointRouting(Object endpoint) {
		return TestUtils
				.getPropertyValue(endpoint, "routingKeyExpression", SpelExpression.class)
				.getExpressionString();
	}

	@Override
	protected String getExpectedRoutingBaseDestination(String name, String group) {
		return name;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	protected void checkRkExpressionForPartitionedModuleSpEL(Object endpoint) {
		assertThat(getEndpointRouting(endpoint))
				.contains(getExpectedRoutingBaseDestination("'part.0'", "test")
						+ " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']");
	}

	@Override
	public Spy spyOn(final String queue) {
		final RabbitTemplate template = new RabbitTemplate(
				this.rabbitTestSupport.getResource());
		template.setAfterReceivePostProcessors(
				new DelegatingDecompressingPostProcessor());
		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				if (expectNull) {
					Thread.sleep(50);
					return template.receiveAndConvert(
							new RabbitConsumerProperties().getPrefix() + queue);
				}
				Object bar = null;
				int n = 0;
				while (n++ < 100 && bar == null) {
					bar = template.receiveAndConvert(
							new RabbitConsumerProperties().getPrefix() + queue);
					Thread.sleep(100);
				}
				assertThat(n).isLessThan(100)
						.withFailMessage("Message did not arrive in RabbitMQ");
				return bar;
			}

		};
	}

	private RuntimeException bigCause(RuntimeException cause) {
		if (getStackTraceAsString(cause).length() > this.maxStackTraceSize) {
			return cause;
		}
		return bigCause(new RuntimeException(BIG_EXCEPTION_MESSAGE, cause));
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

	private void verifyAutoDeclareContextClear(RabbitTestBinder binder) {
		ApplicationContext ctx =
				TestUtils.getPropertyValue(binder, "binder.provisioningProvider.autoDeclareContext",
						ApplicationContext.class);
		assertThat(ctx.getBeansOfType(Declarable.class)).isEmpty();
	}

	private List<Map<String, Object>> getBindingsBySource(String vhost, String name) {
		return RestUtils.getBindingsBySource(client, uri, vhost, name);
	}

	private Map<String, Object> getExchange(String vhost, String name) {
		return RestUtils.getExchange(client, uri, vhost, name);
	}

	private Map<String, Object> getQueue(String vhost, String name) {
		return RestUtils.getQueue(client, uri, vhost, name);
	}

	private boolean requiredConsumerState(long state, Map<String, Object> queue) {
		Object consumers = queue.get("consumers");
		return state == 0
				? consumers == null || (Integer) consumers == 0
				: consumers != null && (Integer) consumers == state;
	}

	public static class TestPartitionKeyExtractorClass
			implements PartitionKeyExtractorStrategy {

		@Override
		public Object extractKey(Message<?> message) {
			return 0;
		}

	}

	public static class TestPartitionSelectorClass implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			return 0;
		}

	}

	public static class Pojo {

		private String field;

		public Pojo() {
			super();
		}

		public Pojo(String field) {
			this.field = field;
		}

		public String getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field = field;
		}

	}
	// @checkstyle:on

	public static class TestBatchingStrategy implements BatchingStrategy {

		private final List<org.springframework.amqp.core.Message> messages = new ArrayList<>();
		private String exchange;
		private String routingKey;
		private int currentSize;

		@Override
		public MessageBatch addToBatch(String exchange, String routingKey, org.springframework.amqp.core.Message message) {
			this.exchange = exchange;
			this.routingKey = routingKey;
			this.messages.add(message);
			currentSize += message.getBody().length + 2;

			MessageBatch batch = null;
			if (this.messages.size() == 5) {
				batch = this.doReleaseBatch();
			}

			return batch;
		}

		@Override
		public Date nextRelease() {
			return null;
		}

		@Override
		public Collection<MessageBatch> releaseBatches() {
			MessageBatch batch = this.doReleaseBatch();
			return batch == null ? Collections.emptyList() : Collections.singletonList(batch);
		}

		private MessageBatch doReleaseBatch() {
			if (this.messages.size() < 1) {
				return null;
			}
			else {
				ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[this.currentSize]);
				for (org.springframework.amqp.core.Message message: messages) {
					byteBuffer.put(message.getBody()).putChar('\n');
				}
				MessageBatch messageBatch = new MessageBatch(this.exchange, this.routingKey,
						new org.springframework.amqp.core.Message(byteBuffer.array(), new MessageProperties()));
				this.messages.clear();
				return messageBatch;
			}
		}
	}

}
