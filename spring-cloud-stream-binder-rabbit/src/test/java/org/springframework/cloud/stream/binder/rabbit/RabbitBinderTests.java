/*
 * Copyright 2013-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitManagementTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;

import com.rabbitmq.http.client.domain.QueueInfo;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author David Turanski
 */
public class RabbitBinderTests extends
		PartitionCapableBinderTests<RabbitTestBinder, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = RabbitMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Rule
	public RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport(true);

	@Rule
	public TestName testName = new TestName();

	@Override
	protected RabbitTestBinder getBinder() {
		if (testBinder == null) {
			testBinder = new RabbitTestBinder(rabbitAvailableRule.getResource(), new RabbitProperties());
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<RabbitConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new RabbitConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<RabbitProducerProperties> createProducerProperties() {
		ExtendedProducerProperties<RabbitProducerProperties> props = new ExtendedProducerProperties<>(
				new RabbitProducerProperties());
		if (testName.getMethodName().equals("testPartitionedModuleSpEL")) {
			props.getExtension().setRoutingKeyExpression("'part.0'");
		}
		return props;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testSendAndReceiveBad() throws Exception {
		RabbitTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0", moduleOutputChannel,
				createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test", moduleInputChannel,
				createConsumerProperties());
		Message<?> message = MessageBuilder.withPayload("bad").setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar")
				.build();
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
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerProperties() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setRequeueRejected(true);
		properties.getExtension().setTransacted(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("props.0", null,
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
		assertThat(container.getQueueNames()[0]).startsWith(properties.getExtension().getPrefix());
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers")).isNull();
		assertThat(TestUtils.getPropertyValue(container, "defaultRequeueRejected", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "prefetchCount")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "txSize")).isEqualTo(1);
		Advice retry = TestUtils.getPropertyValue(container, "adviceChain", Advice[].class)[0];
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.retryPolicy.maxAttempts")).isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.initialInterval")).isEqualTo(1000L);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.maxInterval")).isEqualTo(10000L);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.multiplier")).isEqualTo(2.0);
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
		properties.getExtension().setRequestHeaderPatterns(new String[] { "foo" });
		properties.getExtension().setTxSize(10);
		properties.setInstanceIndex(0);
		consumerBinding = binder.bindConsumer("props.0", "test", createBindableChannel("input", new BindingProperties()),
				properties);

		endpoint = extractEndpoint(consumerBinding);
		container = verifyContainer(endpoint);

		assertThat(container.getQueueNames()[0]).isEqualTo("foo.props.0.test");

		consumerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();
	}

	@Test
	public void testConsumerPropertiesWithUserInfrastructureNoBind() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());
		Queue queue = new Queue("propsUser1.infra");
		admin.declareQueue(queue);
		DirectExchange exchange = new DirectExchange("propsUser1");
		admin.declareExchange(exchange);
		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("foo"));

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setDeclareExchange(false);
		properties.getExtension().setBindQueue(false);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser1", "infra",
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		RabbitManagementTemplate rmt = new RabbitManagementTemplate();
		List<org.springframework.amqp.core.Binding> bindings = rmt.getBindingsForExchange("/", exchange.getName());
		assertThat(bindings.size()).isEqualTo(1);
	}

	@Test
	public void testConsumerPropertiesWithUserInfrastructureCustomExchangeAndRK() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setExchangeType(ExchangeTypes.DIRECT);
		properties.getExtension().setBindingRoutingKey("foo");
//		properties.getExtension().setDelayedExchange(true); // requires delayed message exchange plugin; tested locally

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser2", "infra",
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		RabbitManagementTemplate rmt = new RabbitManagementTemplate();
		List<org.springframework.amqp.core.Binding> bindings = rmt.getBindingsForExchange("/", "propsUser2");
		int n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = rmt.getBindingsForExchange("/", "propsUser2");
		}
		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).getExchange()).isEqualTo("propsUser2");
		assertThat(bindings.get(0).getDestination()).isEqualTo("propsUser2.infra");
		assertThat(bindings.get(0).getRoutingKey()).isEqualTo("foo");

//		// TODO: AMQP-696
//		// Exchange exchange = rmt.getExchange("propsUser2");
//		ExchangeInfo ei = rmt.getClient().getExchange("/", "propsUser2"); // requires delayed message exchange plugin
//		assertThat(ei.getType()).isEqualTo("x-delayed-message");
//		assertThat(ei.getArguments().get("x-delayed-type")).isEqualTo("direct");

		Exchange exchange = rmt.getExchange("propsUser2");
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = rmt.getExchange("propsUser2");
		}
		assertThat(exchange).isInstanceOf(DirectExchange.class);
		assertThat(exchange.isDurable()).isEqualTo(true);
		assertThat(exchange.isAutoDelete()).isEqualTo(false);
	}

	@Test
	public void testConsumerPropertiesWithUserInfrastructureCustomQueueArgs() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		RabbitConsumerProperties extProps = properties.getExtension();
		extProps.setExchangeType(ExchangeTypes.DIRECT);
		extProps.setExchangeDurable(false);
		extProps.setExchangeAutoDelete(true);
		extProps.setBindingRoutingKey("foo");
		extProps.setExpires(30_000);
		extProps.setMaxLength(10_000);
		extProps.setMaxLengthBytes(100_000);
		extProps.setMaxPriority(10);
		extProps.setTtl(2_000);
		extProps.setAutoBindDlq(true);
		extProps.setDeadLetterQueueName("customDLQ");
		extProps.setDeadLetterExchange("customDLX");
		extProps.setDeadLetterRoutingKey("customDLRK");
		extProps.setDlqDeadLetterExchange("propsUser3");
		extProps.setDlqDeadLetterRoutingKey("propsUser3");
		extProps.setDlqExpires(60_000);
		extProps.setDlqMaxLength(20_000);
		extProps.setDlqMaxLengthBytes(40_000);
		extProps.setDlqMaxPriority(8);
		extProps.setDlqTtl(1_000);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("propsUser3", "infra",
				createBindableChannel("input", new BindingProperties()), properties);
		Lifecycle endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.isRunning()).isTrue();
		consumerBinding.unbind();
		assertThat(container.isRunning()).isFalse();
		RabbitManagementTemplate rmt = new RabbitManagementTemplate();
		List<org.springframework.amqp.core.Binding> bindings = rmt.getBindingsForExchange("/", "propsUser3");
		int n = 0;
		while (n++ < 100 && bindings == null || bindings.size() < 1) {
			Thread.sleep(100);
			bindings = rmt.getBindingsForExchange("/", "propsUser3");
		}
		assertThat(bindings.size()).isEqualTo(1);
		assertThat(bindings.get(0).getExchange()).isEqualTo("propsUser3");
		assertThat(bindings.get(0).getDestination()).isEqualTo("propsUser3.infra");
		assertThat(bindings.get(0).getRoutingKey()).isEqualTo("foo");

		Exchange exchange = rmt.getExchange("propsUser3");
		n = 0;
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = rmt.getExchange("propsUser3");
		}
		assertThat(exchange).isInstanceOf(DirectExchange.class);
		assertThat(exchange.isDurable()).isEqualTo(false);
		assertThat(exchange.isAutoDelete()).isEqualTo(true);

//		Queue queue = rmt.getQueue("propsUser3"); AMQP-698
		QueueInfo queue = rmt.getClient().getQueue("/", "propsUser3.infra");
		n = 0;
		while (n++ < 100 && queue == null) {
			Thread.sleep(100);
			queue = rmt.getClient().getQueue("/", "propsUser3.infra");
		}
		assertThat(queue).isNotNull();
		Map<String, Object> args = queue.getArguments();
		assertThat(args.get("x-expires")).isEqualTo(30_000);
		assertThat(args.get("x-max-length")).isEqualTo(10_000);
		assertThat(args.get("x-max-length-bytes")).isEqualTo(100_000);
		assertThat(args.get("x-max-priority")).isEqualTo(10);
		assertThat(args.get("x-message-ttl")).isEqualTo(2_000);
		assertThat(args.get("x-dead-letter-exchange")).isEqualTo("customDLX");
		assertThat(args.get("x-dead-letter-routing-key")).isEqualTo("customDLRK");

		queue = rmt.getClient().getQueue("/", "customDLQ");

		n = 0;
		while (n++ < 100 && queue == null) {
			Thread.sleep(100);
			queue = rmt.getClient().getQueue("/", "customDLQ");
		}
		assertThat(queue).isNotNull();
		args = queue.getArguments();
		assertThat(args.get("x-expires")).isEqualTo(60_000);
		assertThat(args.get("x-max-length")).isEqualTo(20_000);
		assertThat(args.get("x-max-length-bytes")).isEqualTo(40_000);
		assertThat(args.get("x-max-priority")).isEqualTo(8);
		assertThat(args.get("x-message-ttl")).isEqualTo(1_000);
		assertThat(args.get("x-dead-letter-exchange")).isEqualTo("propsUser3");
		assertThat(args.get("x-dead-letter-routing-key")).isEqualTo("propsUser3");
	}

	@Test
	public void testProducerProperties() throws Exception {
		RabbitTestBinder binder = getBinder();
		Binding<MessageChannel> producerBinding = binder.bindProducer("props.0",
				createBindableChannel("input", new BindingProperties()),
				createProducerProperties());
		Lifecycle endpoint = extractEndpoint(producerBinding);
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertThat(mode).isEqualTo(MessageDeliveryMode.PERSISTENT);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.matchers", List.class);
		assertThat(requestHeaders).hasSize(2);
		producerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(endpoint, "amqpTemplate.transactional", Boolean.class))
				.isFalse();

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setPrefix("foo.");
		producerProperties.getExtension().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setRequestHeaderPatterns(new String[] { "foo" });
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("'foo'"));
		producerProperties.setPartitionKeyExtractorClass(TestPartitionKeyExtractorClass.class);
		producerProperties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("0"));
		producerProperties.setPartitionSelectorClass(TestPartitionSelectorClass.class);
		producerProperties.setPartitionCount(1);
		producerProperties.getExtension().setTransacted(true);
		producerProperties.getExtension().setDelayExpression("42");
		producerProperties.setRequiredGroups("prodPropsRequired");

		BindingProperties producerBindingProperties = createProducerBindingProperties(producerProperties);
		DirectChannel channel = createBindableChannel("output", producerBindingProperties);
		producerBinding = binder.bindProducer("props.0", channel,
				producerProperties);
		endpoint = extractEndpoint(producerBinding);
		assertThat(getEndpointRouting(endpoint))
				.isEqualTo("'props.0-' + headers['" + BinderHeaders.PARTITION_HEADER + "']");
		assertThat(TestUtils.getPropertyValue(endpoint, "delayExpression", SpelExpression.class)
				.getExpressionString()).isEqualTo("42");
		mode = TestUtils.getPropertyValue(endpoint, "defaultDeliveryMode", MessageDeliveryMode.class);
		assertThat(mode).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(TestUtils.getPropertyValue(endpoint, "amqpTemplate.transactional", Boolean.class))
				.isTrue();
		verifyFooRequestProducer(endpoint);
		channel.send(new GenericMessage<>("foo"));
		org.springframework.amqp.core.Message received = new RabbitTemplate(this.rabbitAvailableRule.getResource())
				.receive("foo.props.0.prodPropsRequired-0", 10_000);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedDelay()).isEqualTo(42);

		producerBinding.unbind();
		assertThat(endpoint.isRunning()).isFalse();
	}

	@Test
	public void testDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		RabbitTestBinder binder = getBinder();

		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setDurableSubscription(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("durableTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("durabletest.0", "tgroup", moduleInputChannel,
				consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend(TEST_PREFIX + "durabletest.0", "", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "durabletest.0.tgroup.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("foo");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		consumerBinding.unbind();
		assertThat(admin.getQueueProperties(TEST_PREFIX + "durabletest.0.tgroup.dlq")).isNotNull();
	}

	@Test
	public void testNonDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setDurableSubscription(false);
		consumerProperties.setMaxAttempts(1); // disable retry
		BindingProperties bindingProperties = createConsumerBindingProperties(consumerProperties);
		DirectChannel moduleInputChannel = createBindableChannel("input", bindingProperties);
		moduleInputChannel.setBeanName("nondurabletest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("nondurabletest.0", "tgroup", moduleInputChannel,
				consumerProperties);

		consumerBinding.unbind();
		assertThat(admin.getQueueProperties(TEST_PREFIX + "nondurabletest.0.dlq")).isNull();
	}

	@Test
	public void testAutoBindDLQ() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		BindingProperties bindingProperties = createConsumerBindingProperties(consumerProperties);
		DirectChannel moduleInputChannel = createBindableChannel("input", bindingProperties);
		moduleInputChannel.setBeanName("dlqTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("dlqtest", "default", moduleInputChannel,
				consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqtest.default", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "dlqtest.default.dlq");
			if (deadLetter != null) {
				assertThat(deadLetter).isEqualTo("foo");
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		consumerBinding.unbind();

		ApplicationContext context = TestUtils.getPropertyValue(binder, "binder.provisioningProvider.autoDeclareContext",
				ApplicationContext.class);
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.binding")).isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default")).isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq.binding")).isFalse();
		assertThat(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq")).isFalse();
	}

	@Test
	public void testAutoBindDLQPartionedConsumerFirst() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.setMaxAttempts(1); // disable retry
		properties.setPartitioned(true);
		properties.setInstanceIndex(0);
		DirectChannel input0 = createBindableChannel("input", createConsumerBindingProperties(properties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.0", "default",
				new QueueChannel(), properties);
		properties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1", createConsumerBindingProperties(properties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.0", "default",
				new QueueChannel(), properties);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setPrefix("bindertest.");
		producerProperties.getExtension().setAutoBindDlq(true);
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		producerProperties.setPartitionSelectorClass(PartitionTestSupport.class);
		producerProperties.setPartitionCount(2);
		BindingProperties bindingProperties = createProducerBindingProperties(producerProperties);
		DirectChannel output = createBindableChannel("output", bindingProperties);
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.0", output, producerProperties);

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

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.0.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.0.dlqPartGrp-1");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		output.send(new GenericMessage<>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.0.dlqPartGrp-0");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();
	}

	@Test
	public void testAutoBindDLQPartionedConsumerFirstWithRepublish() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setRepublishToDlq(true);
		properties.getExtension().setRepublishDeliveyMode(MessageDeliveryMode.NON_PERSISTENT);
		properties.setMaxAttempts(1); // disable retry
		properties.setPartitioned(true);
		properties.setInstanceIndex(0);
		DirectChannel input0 = createBindableChannel("input", createConsumerBindingProperties(properties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partPubDLQ.0", "dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partPubDLQ.0", "default",
				new QueueChannel(), properties);
		properties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1", createConsumerBindingProperties(properties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partPubDLQ.0", "dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partPubDLQ.0", "default",
				new QueueChannel(), properties);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setPrefix("bindertest.");
		producerProperties.getExtension().setAutoBindDlq(true);
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		producerProperties.setPartitionSelectorClass(PartitionTestSupport.class);
		producerProperties.setPartitionCount(2);
		BindingProperties bindingProperties = createProducerBindingProperties(producerProperties);
		DirectChannel output = createBindableChannel("output", bindingProperties);
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partPubDLQ.0", output, producerProperties);

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

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partPubDLQ.0.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getHeaders().get("x-original-routingKey"))
				.isEqualTo("partPubDLQ.0-1");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);
		assertThat(received.getMessageProperties().getReceivedDeliveryMode())
				.isEqualTo(MessageDeliveryMode.NON_PERSISTENT);

		output.send(new GenericMessage<>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getHeaders().get("x-original-routingKey"))
			.isEqualTo("partPubDLQ.0-0");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();
	}

	@Test
	public void testAutoBindDLQPartitionedProducerFirst() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> properties = createProducerProperties();

		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.setRequiredGroups("dlqPartGrp");
		properties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		properties.setPartitionSelectorClass(PartitionTestSupport.class);
		properties.setPartitionCount(2);
		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(properties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.1", output, properties);

		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix("bindertest.");
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.setPartitioned(true);
		consumerProperties.setInstanceIndex(0);
		DirectChannel input0 = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input0,
				consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.1", "defaultConsumer",
				new QueueChannel(), consumerProperties);
		consumerProperties.setInstanceIndex(1);
		DirectChannel input1 = createBindableChannel("input1", createConsumerBindingProperties(consumerProperties));
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input1,
				consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.1", "defaultConsumer",
				new QueueChannel(), consumerProperties);

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

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.1.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
				.isEqualTo("bindertest.partDLQ.1.dlqPartGrp-1");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);
		assertThat(received.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);

		output.send(new GenericMessage<Integer>(0));
		received = template.receive(streamDLQName);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey())
			.isEqualTo("bindertest.partDLQ.1.dlqPartGrp-0");
		assertThat(received.getMessageProperties().getHeaders()).doesNotContainKey(BinderHeaders.PARTITION_HEADER);

		input0Binding.unbind();
		input1Binding.unbind();
		defaultConsumerBinding1.unbind();
		defaultConsumerBinding2.unbind();
		outputBinding.unbind();
	}

	@Test
	public void testAutoBindDLQwithRepublish() throws Exception {
		// pre-declare the queue with dead-lettering, users can also use a policy
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", TEST_PREFIX + "DLX");
		args.put("x-dead-letter-routing-key", TEST_PREFIX + "dlqpubtest.default");
		Queue queue = new Queue(TEST_PREFIX + "dlqpubtest.default", true, false, false, args);
		admin.declareQueue(queue);

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix(TEST_PREFIX);
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.getExtension().setRepublishToDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		moduleInputChannel.setBeanName("dlqPubTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.dlqpubtest", "foo", moduleInputChannel,
				consumerProperties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "foo.dlqpubtest.foo", "foo");

		int n = 0;
		while (n++ < 100) {
			org.springframework.amqp.core.Message deadLetter = template.receive(TEST_PREFIX + "foo.dlqpubtest.foo.dlq");
			if (deadLetter != null) {
				assertThat(new String(deadLetter.getBody())).isEqualTo("foo");
				assertThat(deadLetter.getMessageProperties().getHeaders()).containsKey(("x-exception-stacktrace"));
				break;
			}
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);

		consumerBinding.unbind();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchingAndCompression() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		producerProperties.getExtension().setBatchingEnabled(true);
		producerProperties.getExtension().setBatchSize(2);
		producerProperties.getExtension().setBatchBufferLimit(100000);
		producerProperties.getExtension().setBatchTimeout(30000);
		producerProperties.getExtension().setCompress(true);
		producerProperties.setRequiredGroups("default");

		DirectChannel output = createBindableChannel("input", createProducerBindingProperties(producerProperties));
		output.setBeanName("batchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("batching.0", output, producerProperties);

		Log logger = spy(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor"))
				.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertThat(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.level"))
				.isEqualTo(Deflater.BEST_SPEED);

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Object out = spyOn("batching.0.default").receive(false);
		assertThat(out).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) out)).isEqualTo("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar");

		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).trace(captor.capture());
		assertThat(captor.getValue().toString()).contains(("Compressed 14 to "));

		QueueChannel input = new QueueChannel();
		input.setBeanName("batchingConsumer");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("batching.0", "test", input,
				createConsumerProperties());

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
	}

	/*
	 * Test late binding due to broker down; queues with and without DLQs, and partitioned
	 * queues.
	 */
	@Test
	public void testLateBinding() throws Exception {
		RabbitTestSupport.RabbitProxy proxy = new RabbitTestSupport.RabbitProxy();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost", proxy.getPort());

		RabbitMessageChannelBinder rabbitBinder = new RabbitMessageChannelBinder(cf, new RabbitProperties(),
				new RabbitExchangeQueueProvisioner(cf));
		RabbitTestBinder binder = new RabbitTestBinder(cf, rabbitBinder);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setPrefix("latebinder.");
		producerProperties.getExtension().setAutoBindDlq(true);

		MessageChannel moduleOutputChannel = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> late0ProducerBinding = binder.bindProducer("late.0", moduleOutputChannel, producerProperties);

		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> rabbitConsumerProperties = createConsumerProperties();
		rabbitConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> late0ConsumerBinding = binder.bindConsumer("late.0", "test", moduleInputChannel,
				rabbitConsumerProperties);

		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload.equals('0') ? 0 : 1"));
		producerProperties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		producerProperties.setPartitionCount(2);

		MessageChannel partOutputChannel = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> partlate0ProducerBinding = binder.bindProducer("partlate.0", partOutputChannel,
				producerProperties);

		QueueChannel partInputChannel0 = new QueueChannel();
		QueueChannel partInputChannel1 = new QueueChannel();

		ExtendedConsumerProperties<RabbitConsumerProperties> partLateConsumerProperties = createConsumerProperties();
		partLateConsumerProperties.getExtension().setPrefix("latebinder.");
		partLateConsumerProperties.setPartitioned(true);
		partLateConsumerProperties.setInstanceIndex(0);
		Binding<MessageChannel> partlate0Consumer0Binding = binder.bindConsumer("partlate.0", "test", partInputChannel0,
				partLateConsumerProperties);
		partLateConsumerProperties.setInstanceIndex(1);
		Binding<MessageChannel> partlate0Consumer1Binding = binder.bindConsumer("partlate.0", "test", partInputChannel1,
				partLateConsumerProperties);

		ExtendedProducerProperties<RabbitProducerProperties> noDlqProducerProperties = createProducerProperties();
		noDlqProducerProperties.getExtension().setPrefix("latebinder.");
		MessageChannel noDLQOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(noDlqProducerProperties));
		Binding<MessageChannel> noDlqProducerBinding = binder.bindProducer("lateNoDLQ.0", noDLQOutputChannel,
				noDlqProducerProperties);

		QueueChannel noDLQInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> noDlqConsumerProperties = createConsumerProperties();
		noDlqConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> noDlqConsumerBinding = binder.bindConsumer("lateNoDLQ.0", "test", noDLQInputChannel,
				noDlqConsumerProperties);

		MessageChannel outputChannel = createBindableChannel("output", createProducerBindingProperties(noDlqProducerProperties));
		Binding<MessageChannel> pubSubProducerBinding = binder.bindProducer("latePubSub", outputChannel,
				noDlqProducerProperties);
		QueueChannel pubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(false);
		Binding<MessageChannel> nonDurableConsumerBinding = binder.bindConsumer("latePubSub", "lategroup",
				pubSubInputChannel, noDlqConsumerProperties);
		QueueChannel durablePubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(true);
		Binding<MessageChannel> durableConsumerBinding = binder.bindConsumer("latePubSub", "lateDurableGroup",
				durablePubSubInputChannel, noDlqConsumerProperties);

		proxy.start();

		moduleOutputChannel.send(new GenericMessage<>("foo"));
		Message<?> message = moduleInputChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isNotNull();

		noDLQOutputChannel.send(new GenericMessage<>("bar"));
		message = noDLQInputChannel.receive(10000);
		assertThat(message);
		assertThat(message.getPayload()).isEqualTo("bar");

		outputChannel.send(new GenericMessage<>("baz"));
		message = pubSubInputChannel.receive(10000);
		assertThat(message);
		assertThat(message.getPayload()).isEqualTo("baz");
		message = durablePubSubInputChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("baz");

		partOutputChannel.send(new GenericMessage<>("0"));
		partOutputChannel.send(new GenericMessage<>("1"));
		message = partInputChannel0.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("0");
		message = partInputChannel1.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("1");

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

		this.rabbitAvailableRule.getResource().destroy();
	}

	private SimpleMessageListenerContainer verifyContainer(Lifecycle endpoint) {
		SimpleMessageListenerContainer container;
		Advice retry;
		container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.NONE);
		assertThat(container.getQueueNames()[0]).startsWith("foo.props.0");
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class)).isFalse();
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(2);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers")).isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(container, "defaultRequeueRejected", Boolean.class)).isFalse();
		assertThat(TestUtils.getPropertyValue(container, "prefetchCount")).isEqualTo(20);
		assertThat(TestUtils.getPropertyValue(container, "txSize")).isEqualTo(10);
		retry = TestUtils.getPropertyValue(container, "adviceChain", Advice[].class)[0];
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.retryPolicy.maxAttempts")).isEqualTo(23);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.initialInterval")).isEqualTo(2000L);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.maxInterval")).isEqualTo(20000L);
		assertThat(TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.multiplier")).isEqualTo(5.0);

		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint, "headerMapper.requestHeaderMatcher.matchers",
				List.class);
		assertThat(requestMatchers).hasSize(1);
		assertThat(TestUtils.getPropertyValue(requestMatchers.get(0), "pattern")).isEqualTo("foo");

		return container;
	}

	private void verifyFooRequestProducer(Lifecycle endpoint) {
		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.matchers", List.class);
		assertThat(requestMatchers).hasSize(2);
		assertThat(TestUtils.getPropertyValue(requestMatchers.get(1), "pattern")).isEqualTo("foo");
	}

	@Override
	protected String getEndpointRouting(Object endpoint) {
		return TestUtils.getPropertyValue(endpoint, "routingKeyExpression", SpelExpression.class)
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
		assertThat(getEndpointRouting(endpoint)).contains(getExpectedRoutingBaseDestination("'part.0'", "test")
				+ " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']");
	}

	@Override
	public Spy spyOn(final String queue) {
		final RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setAfterReceivePostProcessors(new DelegatingDecompressingPostProcessor());
		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				if (expectNull) {
					Thread.sleep(50);
					return template.receiveAndConvert(new RabbitConsumerProperties().getPrefix() + queue);
				}
				Object bar = null;
				int n = 0;
				while (n++ < 100 && bar == null) {
					bar = template.receiveAndConvert(new RabbitConsumerProperties().getPrefix() + queue);
					Thread.sleep(100);
				}
				assertThat(n).isLessThan(100).withFailMessage("Message did not arrive in RabbitMQ");
				return bar;
			}

		};
	}

	public static class TestPartitionKeyExtractorClass implements PartitionKeyExtractorStrategy {

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

}
