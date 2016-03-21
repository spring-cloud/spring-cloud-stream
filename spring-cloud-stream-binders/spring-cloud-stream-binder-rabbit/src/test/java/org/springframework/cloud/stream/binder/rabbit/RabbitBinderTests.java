/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
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
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.test.junit.rabbit.RabbitTestSupport;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author David Turanski
 */
public class RabbitBinderTests extends PartitionCapableBinderTests<RabbitTestBinder, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = RabbitMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Rule
	public RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport();

	@Override
	protected RabbitTestBinder getBinder() {
		if (testBinder == null) {
			testBinder = new RabbitTestBinder(rabbitAvailableRule.getResource());
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<RabbitConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new RabbitConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<RabbitProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new RabbitProducerProperties());
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testSendAndReceiveBad() throws Exception {
		RabbitTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0", moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test", moduleInputChannel, createConsumerProperties());
		Message<?> message = MessageBuilder.withPayload("bad").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		final CountDownLatch latch = new CountDownLatch(3);
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
				throw new RuntimeException("bad");
			}
		});
		moduleOutputChannel.send(message);
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerProperties() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setTransacted(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("props.0", null, new DirectChannel(), properties);
		AbstractEndpoint endpoint = extractEndpoint(consumerBinding);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.AUTO, container.getAcknowledgeMode());
		assertThat(container.getQueueNames()[0],
				startsWith(properties.getExtension().getPrefix()));
		assertTrue(TestUtils.getPropertyValue(container, "transactional", Boolean.class));
		assertEquals(1, TestUtils.getPropertyValue(container, "concurrentConsumers"));
		assertNull(TestUtils.getPropertyValue(container, "maxConcurrentConsumers"));
		assertTrue(TestUtils.getPropertyValue(container, "defaultRequeueRejected", Boolean.class));
		assertEquals(1, TestUtils.getPropertyValue(container, "prefetchCount"));
		assertEquals(1, TestUtils.getPropertyValue(container, "txSize"));
		Advice retry = TestUtils.getPropertyValue(container, "adviceChain", Advice[].class)[0];
		assertEquals(3, TestUtils.getPropertyValue(retry, "retryOperations.retryPolicy.maxAttempts"));
		assertEquals(1000L, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.initialInterval"));
		assertEquals(10000L, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.maxInterval"));
		assertEquals(2.0, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.multiplier"));
		consumerBinding.unbind();
		assertFalse(endpoint.isRunning());

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
		properties.getExtension().setRequestHeaderPatterns(new String[] {"foo"});
		properties.getExtension().setRequeueRejected(false);
		properties.getExtension().setTxSize(10);
		properties.setInstanceIndex(0);
		consumerBinding = binder.bindConsumer("props.0", "test", new DirectChannel(), properties);

		endpoint = extractEndpoint(consumerBinding);
		container = verifyContainer(endpoint);

		assertEquals("foo.props.0.test", container.getQueueNames()[0]);

		consumerBinding.unbind();
		assertFalse(endpoint.isRunning());
	}

	@Test
	public void testProducerProperties() throws Exception {
		RabbitTestBinder binder = getBinder();
		Binding<MessageChannel> producerBinding = binder.bindProducer("props.0", new DirectChannel(), createProducerProperties());
		@SuppressWarnings("unchecked")
		AbstractEndpoint endpoint = extractEndpoint(producerBinding);
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.PERSISTENT, mode);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.requestHeaderMatcher.strategies", List.class);
		assertEquals(2, requestHeaders.size());
		producerBinding.unbind();
		assertFalse(endpoint.isRunning());
		assertFalse(TestUtils.getPropertyValue(endpoint, "handler.delegate.amqpTemplate.transactional", Boolean.class));

		ExtendedProducerProperties<RabbitProducerProperties> properties = createProducerProperties();
		properties.getExtension().setPrefix("foo.");
		properties.getExtension().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		properties.getExtension().setRequestHeaderPatterns(new String[] {"foo"});
		properties.setPartitionKeyExpression(spelExpressionParser.parseExpression("'foo'"));
		properties.setPartitionKeyExtractorClass(TestPartitionKeyExtractorClass.class);
		properties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("0"));
		properties.setPartitionSelectorClass(TestPartitionSelectorClass.class);
		properties.setPartitionCount(1);
		properties.getExtension().setTransacted(true);

		producerBinding = binder.bindProducer("props.0", new DirectChannel(), properties);
		endpoint = extractEndpoint(producerBinding);
		assertEquals(
				"'props.0-' + headers['partition']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression",
						SpelExpression.class).getExpressionString());
		mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);
		assertTrue(TestUtils.getPropertyValue(endpoint, "handler.delegate.amqpTemplate.transactional", Boolean.class));
		verifyFooRequestProducer(endpoint);

		producerBinding.unbind();
		assertFalse(endpoint.isRunning());
	}

	@Test
	public void testDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		RabbitTestBinder binder = getBinder();

		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix(TEST_PREFIX);
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setDurableSubscription(true);
		properties.setMaxAttempts(1); // disable retry
		properties.getExtension().setRequeueRejected(false);
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("durableTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("durabletest.0", "tgroup", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend(TEST_PREFIX + "durabletest.0", "", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "durabletest.0.tgroup.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		consumerBinding.unbind();
		assertNotNull(admin.getQueueProperties(TEST_PREFIX + "durabletest.0.tgroup.dlq"));
	}

	@Test
	public void testNonDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix(TEST_PREFIX);
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setDurableSubscription(false);
		properties.setMaxAttempts(1); // disable retry
		properties.getExtension().setRequeueRejected(false);
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("nondurabletest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("nondurabletest.0", "tgroup", moduleInputChannel, properties);

		consumerBinding.unbind();
		assertNull(admin.getQueueProperties(TEST_PREFIX + "nondurabletest.0.dlq"));
	}

	@Test
	public void testAutoBindDLQ() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix(TEST_PREFIX);
		properties.getExtension().setAutoBindDlq(true);
		properties.setMaxAttempts(1); // disable retry
		properties.getExtension().setRequeueRejected(false);
		properties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("dlqTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("dlqtest", "default", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqtest.default", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "dlqtest.default.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		consumerBinding.unbind();

		ApplicationContext context = TestUtils.getPropertyValue(binder, "binder.autoDeclareContext",
				ApplicationContext.class);
		assertFalse(context.containsBean(TEST_PREFIX + "dlqtest.default.binding"));
		assertFalse(context.containsBean(TEST_PREFIX + "dlqtest.default"));
		assertFalse(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq.binding"));
		assertFalse(context.containsBean(TEST_PREFIX + "dlqtest.default.dlq"));
	}

	@Test
	public void testAutoBindDLQPartionedConsumerFirst() throws Exception {
		RabbitTestBinder binder = getBinder();
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix("bindertest.");
		properties.getExtension().setAutoBindDlq(true);
		properties.setMaxAttempts(1); // disable retry
		properties.getExtension().setRequeueRejected(false);
		properties.setPartitioned(true);
		properties.setInstanceIndex(0);
		DirectChannel input0 = new DirectChannel();
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 =
				binder.bindConsumer("partDLQ.0", "default", new QueueChannel(), properties);
		properties.setInstanceIndex(1);
		DirectChannel input1 = new DirectChannel();
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.0", "default", new QueueChannel(), properties);

		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setPrefix("bindertest.");
		producerProperties.getExtension().setAutoBindDlq(true);
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		producerProperties.setPartitionSelectorClass(PartitionTestSupport.class);
		producerProperties.setPartitionCount(2);
		DirectChannel output = new DirectChannel();
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

		output.send(new GenericMessage<Integer>(1));
		assertTrue(latch1.await(10, TimeUnit.SECONDS));

		output.send(new GenericMessage<Integer>(0));
		assertTrue(latch0.await(10, TimeUnit.SECONDS));

		output.send(new GenericMessage<Integer>(1));

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.0.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertNotNull(received);
		assertEquals(1, received.getMessageProperties().getHeaders().get("partition"));

		output.send(new GenericMessage<Integer>(0));
		received = template.receive(streamDLQName);
		assertNotNull(received);
		assertEquals(0, received.getMessageProperties().getHeaders().get("partition"));

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
		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.1", output, properties);

		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setPrefix("bindertest.");
		consumerProperties.getExtension().setAutoBindDlq(true);
		consumerProperties.setMaxAttempts(1); // disable retry
		consumerProperties.getExtension().setRequeueRejected(false);
		consumerProperties.setPartitioned(true);
		consumerProperties.setInstanceIndex(0);
		DirectChannel input0 = new DirectChannel();
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input0, consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.1", "defaultConsumer", new QueueChannel(), consumerProperties);
		consumerProperties.setInstanceIndex(1);
		DirectChannel input1 = new DirectChannel();
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input1, consumerProperties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.1", "defaultConsumer", new QueueChannel(), consumerProperties);

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
		assertTrue(latch1.await(10, TimeUnit.SECONDS));

		output.send(new GenericMessage<Integer>(0));
		assertTrue(latch0.await(10, TimeUnit.SECONDS));

		output.send(new GenericMessage<Integer>(1));

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setReceiveTimeout(10000);

		String streamDLQName = "bindertest.partDLQ.1.dlqPartGrp.dlq";

		org.springframework.amqp.core.Message received = template.receive(streamDLQName);
		assertNotNull(received);
		assertEquals(1, received.getMessageProperties().getHeaders().get("partition"));

		output.send(new GenericMessage<Integer>(0));
		received = template.receive(streamDLQName);
		assertNotNull(received);
		assertEquals(0, received.getMessageProperties().getHeaders().get("partition"));

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
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setPrefix(TEST_PREFIX);
		properties.getExtension().setAutoBindDlq(true);
		properties.getExtension().setRepublishToDlq(true);
		properties.setMaxAttempts(1); // disable retry
		properties.getExtension().setRequeueRejected(false);
		properties.getExtension().setDurableSubscription(true);
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("dlqPubTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("dlqpubtest", "default", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqpubtest.default", "foo");

		int n = 0;
		while (n++ < 100) {
			org.springframework.amqp.core.Message deadLetter = template.receive(TEST_PREFIX + "dlqpubtest.default.dlq");
			if (deadLetter != null) {
				assertEquals("foo", new String(deadLetter.getBody()));
				assertNotNull(deadLetter.getMessageProperties().getHeaders().get("x-exception-stacktrace"));
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		consumerBinding.unbind();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchingAndCompression() throws Exception {
		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		RabbitTestBinder binder = getBinder();
		ExtendedProducerProperties<RabbitProducerProperties> properties = createProducerProperties();
		properties.getExtension().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		properties.getExtension().setBatchingEnabled(true);
		properties.getExtension().setBatchSize(2);
		properties.getExtension().setBatchBufferLimit(100000);
		properties.getExtension().setBatchTimeout(30000);
		properties.getExtension().setCompress(true);
		properties.setRequiredGroups("default");

		DirectChannel output = new DirectChannel();
		output.setBeanName("batchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("batching.0", output, properties);

		while (template.receive(properties.getExtension().getPrefix() + "batching.0.default") != null) {
		}

		Log logger = spy(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor"))
				.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertEquals(Deflater.BEST_SPEED, TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.level"));

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Object out = spyOn("batching.0.default").receive(false);
		assertThat(out, instanceOf(byte[].class));
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar", new String((byte[]) out));

		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).trace(captor.capture());
		assertThat(captor.getValue().toString(), containsString("Compressed 14 to "));

		QueueChannel input = new QueueChannel();
		input.setBeanName("batchingConsumer");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("batching.0", "test", input, createConsumerProperties());

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Message<byte[]> in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("foo", new String(in.getPayload()));
		in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("bar", new String(in.getPayload()));
		assertNull(in.getHeaders().get(AmqpHeaders.DELIVERY_MODE));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	/*
	 * Test late binding due to broker down; queues with and without DLQs, and
	 * partitioned queues.
	 */
	@Test
	public void testLateBinding() throws Exception {
		RabbitTestSupport.RabbitProxy proxy = new RabbitTestSupport.RabbitProxy();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost", proxy.getPort());
		RabbitMessageChannelBinder rabbitBinder = new RabbitMessageChannelBinder(cf);
		RabbitTestBinder binder = new RabbitTestBinder(cf, rabbitBinder);

		ExtendedProducerProperties<RabbitProducerProperties> properties = createProducerProperties();
		properties.getExtension().setPrefix("latebinder.");
		properties.getExtension().setAutoBindDlq(true);

		MessageChannel moduleOutputChannel = new DirectChannel();
		Binding<MessageChannel> late0ProducerBinding = binder.bindProducer("late.0", moduleOutputChannel, properties);

		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> rabbitConsumerProperties = createConsumerProperties();
		rabbitConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> late0ConsumerBinding = binder.bindConsumer("late.0", "test", moduleInputChannel, rabbitConsumerProperties);

		properties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload.equals('0') ? 0 : 1"));
		properties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		properties.setPartitionCount(2);

		MessageChannel partOutputChannel = new DirectChannel();
		Binding<MessageChannel> partlate0ProducerBinding = binder.bindProducer("partlate.0", partOutputChannel, properties);

		QueueChannel partInputChannel0 = new QueueChannel();
		QueueChannel partInputChannel1 = new QueueChannel();

		ExtendedConsumerProperties<RabbitConsumerProperties> partLateConsumerProperties = createConsumerProperties();
		partLateConsumerProperties.getExtension().setPrefix("latebinder.");
		partLateConsumerProperties.setPartitioned(true);
		partLateConsumerProperties.setInstanceIndex(0);
		Binding<MessageChannel> partlate0Consumer0Binding = binder.bindConsumer("partlate.0", "test", partInputChannel0, partLateConsumerProperties);
		partLateConsumerProperties.setInstanceIndex(1);
		Binding<MessageChannel> partlate0Consumer1Binding = binder.bindConsumer("partlate.0", "test", partInputChannel1, partLateConsumerProperties);

		ExtendedProducerProperties<RabbitProducerProperties> noDlqProducerProperties = createProducerProperties();
		noDlqProducerProperties.getExtension().setPrefix("latebinder.");
		MessageChannel noDLQOutputChannel = new DirectChannel();
		Binding<MessageChannel> noDlqProducerBinding = binder.bindProducer("lateNoDLQ.0", noDLQOutputChannel, noDlqProducerProperties);

		QueueChannel noDLQInputChannel = new QueueChannel();
		ExtendedConsumerProperties<RabbitConsumerProperties> noDlqConsumerProperties = createConsumerProperties();
		noDlqConsumerProperties.getExtension().setPrefix("latebinder.");
		Binding<MessageChannel> noDlqConsumerBinding = binder.bindConsumer("lateNoDLQ.0", "test", noDLQInputChannel, noDlqConsumerProperties);

		MessageChannel outputChannel = new DirectChannel();
		Binding<MessageChannel> pubSubProducerBinding = binder.bindProducer("latePubSub", outputChannel, noDlqProducerProperties);
		QueueChannel pubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(false);
		Binding<MessageChannel> nonDurableConsumerBinding = binder.bindConsumer("latePubSub", "lategroup", pubSubInputChannel, noDlqConsumerProperties);
		QueueChannel durablePubSubInputChannel = new QueueChannel();
		noDlqConsumerProperties.getExtension().setDurableSubscription(true);
		Binding<MessageChannel> durableConsumerBinding = binder.bindConsumer("latePubSub", "lateDurableGroup", durablePubSubInputChannel, noDlqConsumerProperties);

		proxy.start();

		moduleOutputChannel.send(new GenericMessage<>("foo"));
		Message<?> message = moduleInputChannel.receive(10000);
		assertNotNull(message);
		assertEquals("foo", message.getPayload());

		noDLQOutputChannel.send(new GenericMessage<>("bar"));
		message = noDLQInputChannel.receive(10000);
		assertNotNull(message);
		assertEquals("bar", message.getPayload());

		outputChannel.send(new GenericMessage<>("baz"));
		message = pubSubInputChannel.receive(10000);
		assertNotNull(message);
		assertEquals("baz", message.getPayload());
		message = durablePubSubInputChannel.receive(10000);
		assertNotNull(message);
		assertEquals("baz", message.getPayload());

		partOutputChannel.send(new GenericMessage<>("0"));
		partOutputChannel.send(new GenericMessage<>("1"));
		message = partInputChannel0.receive(10000);
		assertNotNull(message);
		assertEquals("0", message.getPayload());
		message = partInputChannel1.receive(10000);
		assertNotNull(message);
		assertEquals("1", message.getPayload());

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

	private SimpleMessageListenerContainer verifyContainer(AbstractEndpoint endpoint) {
		SimpleMessageListenerContainer container;
		Advice retry;
		container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.NONE, container.getAcknowledgeMode());
		assertThat(container.getQueueNames()[0], startsWith("foo.props.0"));
		assertFalse(TestUtils.getPropertyValue(container, "transactional", Boolean.class));
		assertEquals(2, TestUtils.getPropertyValue(container, "concurrentConsumers"));
		assertEquals(3, TestUtils.getPropertyValue(container, "maxConcurrentConsumers"));
		assertFalse(TestUtils.getPropertyValue(container, "defaultRequeueRejected", Boolean.class));
		assertEquals(20, TestUtils.getPropertyValue(container, "prefetchCount"));
		assertEquals(10, TestUtils.getPropertyValue(container, "txSize"));
		retry = TestUtils.getPropertyValue(container, "adviceChain", Advice[].class)[0];
		assertEquals(23, TestUtils.getPropertyValue(retry, "retryOperations.retryPolicy.maxAttempts"));
		assertEquals(2000L, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.initialInterval"));
		assertEquals(20000L, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.maxInterval"));
		assertEquals(5.0, TestUtils.getPropertyValue(retry, "retryOperations.backOffPolicy.multiplier"));

		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint,
				"headerMapper.requestHeaderMatcher.strategies",
				List.class);
		assertEquals(1, requestMatchers.size());
		assertEquals("foo",
				TestUtils.getPropertyValue(requestMatchers.get(0), "patterns", Collection.class).iterator().next());

		return container;
	}

	private void verifyFooRequestProducer(AbstractEndpoint endpoint) {
		List<?> requestMatchers = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.requestHeaderMatcher.strategies",
				List.class);
		assertEquals(1, requestMatchers.size());
		assertEquals("foo",
				TestUtils.getPropertyValue(requestMatchers.get(0), "patterns", Collection.class).iterator().next());
	}

	@Override
	protected String getEndpointRouting(AbstractEndpoint endpoint) {
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression",
				SpelExpression.class).getExpressionString();
	}

	@Override
	protected String getExpectedRoutingBaseDestination(String name, String group) {
		return name;
	}

	@Override
	protected String getPubSubEndpointRouting(AbstractEndpoint endpoint) {
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.exchangeNameExpression",
				SpelExpression.class).getExpressionString();
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
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
				assertTrue("Message did not arrive in RabbitMQ", n < 100);
				return bar;
			}

		};
	}

	private static class TestPartitionKeyExtractorClass implements PartitionKeyExtractorStrategy {

		@Override
		public Object extractKey(Message<?> message) {
			return null;
		}
	}

	private static class TestPartitionSelectorClass implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			return 0;
		}
	}

}
