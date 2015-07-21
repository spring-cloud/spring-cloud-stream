/*
 * Copyright 2013-2015 the original author or authors.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Mark Fisher
 * @author Gary Russell
 */
public class RabbitBinderTests extends PartitionCapableBinderTests {

	private final String CLASS_UNDER_TEST_NAME = RabbitMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Rule
	public RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport();

	@Override
	protected Binder getBinder() {
		if (testBinder == null) {
			testBinder = new RabbitTestBinder(rabbitAvailableRule.getResource(), getCodec());
		}
		return testBinder;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testSendAndReceiveBad() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		binder.bindProducer("bad.0", moduleOutputChannel, null);
		binder.bindConsumer("bad.0", moduleInputChannel, null);
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
		binder.unbindConsumers("bad.0");
		binder.unbindProducers("bad.0");
	}

	@Test
	public void testConsumerProperties() throws Exception {
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("transacted", "true"); // test transacted with defaults; not allowed with ackmode NONE
		binder.bindConsumer("props.0", new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.AUTO, container.getAcknowledgeMode());
		assertEquals(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + "props.0", container.getQueueNames()[0]);
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
		binder.unbindConsumers("props.0");
		assertEquals(0, bindings.size());

		properties = new Properties();
		properties.put("ackMode", "NONE");
		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");
		properties.put("maxConcurrency", "3");
		properties.put("prefix", "foo.");
		properties.put("prefetch", "20");
		properties.put("requestHeaderPatterns", "foo");
		properties.put("requeue", "false");
		properties.put("txSize", "10");
		properties.put("partitionIndex", 0);
		binder.bindConsumer("props.0", new DirectChannel(), properties);

		@SuppressWarnings("unchecked")
		List<Binding> bindingsNow = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindingsNow.size());
		endpoint = bindingsNow.get(0).getEndpoint();
		container = verifyContainer(endpoint);

		assertEquals("foo.props.0", container.getQueueNames()[0]);

		try {
			binder.bindPubSubConsumer("dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString(getClassUnderTestName() + " does not support consumer properties: "),
					containsString("partitionIndex"),
					containsString("concurrency"),
					containsString(" for dummy.")));
		}
		try {
			binder.bindConsumer("queue:dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertEquals(getClassUnderTestName() + " does not support consumer property: partitionIndex for queue:dummy.",
					e.getMessage());
		}

		binder.unbindConsumers("props.0");
		assertEquals(0, bindingsNow.size());
	}

	@Test
	public void testProducerProperties() throws Exception {
		Binder binder = getBinder();
		binder.bindProducer("props.0", new DirectChannel(), null);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		assertEquals(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + "props.0", TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKey"));
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.PERSISTENT, mode);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.requestHeaderMatcher.strategies", List.class);
		assertEquals(2, requestHeaders.size());
		binder.unbindProducers("props.0");
		assertEquals(0, bindings.size());

		Properties properties = new Properties();
		properties.put("prefix", "foo.");
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("requestHeaderPatterns", "foo");
		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BinderProperties.NEXT_MODULE_COUNT, "1");

		binder.bindProducer("props.0", new DirectChannel(), properties);
		assertEquals(1, bindings.size());
		endpoint = bindings.get(0).getEndpoint();
		assertEquals(
				"'foo.props.0-' + headers['partition']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression", 
						SpelExpression.class).getExpressionString());
		mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);
		verifyFooRequestProducer(endpoint);

		try {
			binder.bindPubSubProducer("dummy", new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString(getClassUnderTestName() + " does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), containsString("for dummy."));
		}
		try {
			binder.bindProducer("queue:dummy", new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString(getClassUnderTestName() + " does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), containsString("for queue:dummy."));
		}

		binder.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testRequestReplyRequestorProperties() throws Exception {
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", "foo.");
		properties.put("deliveryMode", "NON_PERSISTENT");

		properties.put("requestHeaderPatterns", "foo");
		properties.put("replyHeaderPatterns", "bar");

		properties.put("ackMode", "NONE");
		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");
		properties.put("maxConcurrency", "3");
		properties.put("prefix", "foo.");
		properties.put("prefetch", "20");
		properties.put("requeue", "false");
		properties.put("txSize", "10");

		binder.bindRequestor("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);

		assertEquals(2, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint(); // producer
		assertEquals("foo.props.0.requests",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKey"));
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);
		verifyFooRequestBarReplyProducer(endpoint);

		endpoint = bindings.get(1).getEndpoint(); // consumer

		verifyContainer(endpoint);

		verifyBarReplyConsumer(endpoint);

		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BinderProperties.NEXT_MODULE_COUNT, "1");
		properties.put("partitionIndex", "0");
		try {
			binder.bindRequestor("dummy", null, null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString(getClassUnderTestName() + " does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), allOf(containsString("partitionIndex"), containsString("for dummy.")));
		}

		binder.unbindConsumers("props.0");
		binder.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testRequestReplyReplierProperties() throws Exception {
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", "foo.");
		properties.put("deliveryMode", "NON_PERSISTENT");

		properties.put("requestHeaderPatterns", "foo");
		properties.put("replyHeaderPatterns", "bar");

		properties.put("ackMode", "NONE");
		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");
		properties.put("maxConcurrency", "3");
		properties.put("prefix", "foo.");
		properties.put("prefetch", "20");
		properties.put("requeue", "false");
		properties.put("txSize", "10");

		binder.bindReplier("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);

		assertEquals(2, bindings.size());
		AbstractEndpoint endpoint = bindings.get(1).getEndpoint(); // producer
		assertEquals(
				"headers['amqp_replyTo']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression", 
						SpelExpression.class).getExpressionString());
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);

		verifyFooRequestBarReplyProducer(endpoint);

		endpoint = bindings.get(0).getEndpoint(); // consumer

		verifyContainer(endpoint);

		verifyBarReplyConsumer(endpoint);

		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BinderProperties.NEXT_MODULE_COUNT, "1");
		properties.put("partitionIndex", "0");
		try {
			binder.bindReplier("dummy", null, null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString(getClassUnderTestName() + " does not support consumer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), allOf(containsString("partitionIndex"), containsString("for dummy.")));
		}

		binder.unbindConsumers("props.0");
		binder.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		Binder binder = getBinder();

		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("durableSubscription", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("durableTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		binder.bindPubSubConsumer("teststream.tap:stream:durabletest.0", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend(TEST_PREFIX + "topic.tap:stream:durabletest.0", "", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "teststream.tap:stream:durabletest.0.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		binder.unbindConsumer("teststream.tap:stream:durabletest.0", moduleInputChannel);
		assertNotNull(admin.getQueueProperties(TEST_PREFIX + "teststream.tap:stream:durabletest.0.dlq"));
		admin.deleteQueue(TEST_PREFIX + "teststream.tap:stream:durabletest.0.dlq");
		admin.deleteQueue(TEST_PREFIX + "teststream.tap:stream:durabletest.0");
		admin.deleteExchange(TEST_PREFIX + "topic.tap:stream:durabletest.0");
		admin.deleteExchange(TEST_PREFIX + "DLX");
	}

	@Test
	public void testNonDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("durableSubscription", "false");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("nondurabletest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		binder.bindPubSubConsumer("teststream.tap:stream:nondurabletest.0", moduleInputChannel, properties);

		binder.unbindConsumer("teststream.tap:stream:nondurabletest.0", moduleInputChannel);
		assertNull(admin.getQueueProperties(TEST_PREFIX + "teststream.tap:stream:nondurabletest.0.dlq"));
		admin.deleteQueue(TEST_PREFIX + "teststream.tap:stream:nondurabletest.0");
		admin.deleteExchange(TEST_PREFIX + "topic.tap:stream:nondurabletest.0");
	}

	@Test
	public void testAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("dlqTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		binder.bindConsumer("dlqtest", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqtest", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert(TEST_PREFIX + "dlqtest.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		binder.unbindConsumer("dlqtest", moduleInputChannel);
		admin.deleteQueue(TEST_PREFIX + "dlqtest.dlq");
		admin.deleteQueue(TEST_PREFIX + "dlqtest");
		admin.deleteExchange(TEST_PREFIX + "DLX");
	}

	@Test
	public void testAutoBindDLQwithRepublish() throws Exception {
		// pre-declare the queue with dead-lettering, users can also use a policy
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", TEST_PREFIX + "DLX");
		Queue queue = new Queue(TEST_PREFIX + "dlqpubtest", true, false, false, args);
		admin.declareQueue(queue);

		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("republishToDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("dlqPubTest");
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				throw new RuntimeException("foo");
			}

		});
		binder.bindConsumer("dlqpubtest", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", TEST_PREFIX + "dlqpubtest", "foo");

		int n = 0;
		while (n++ < 100) {
			org.springframework.amqp.core.Message deadLetter = template.receive(TEST_PREFIX + "dlqpubtest.dlq");
			if (deadLetter != null) {
				assertEquals("foo", new String(deadLetter.getBody()));
				assertNotNull(deadLetter.getMessageProperties().getHeaders().get("x-exception-stacktrace"));
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		binder.unbindConsumer("dlqpubtest", moduleInputChannel);
		admin.deleteQueue(TEST_PREFIX + "dlqpubtest.dlq");
		admin.deleteQueue(TEST_PREFIX + "dlqpubtest");
		admin.deleteExchange(TEST_PREFIX + "DLX");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchingAndCompression() throws Exception {
		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("batchingEnabled", "true");
		properties.put("batchSize", "2");
		properties.put("batchBufferLimit", "100000");
		properties.put("batchTimeout", "30000");
		properties.put("compress", "true");

		DirectChannel output = new DirectChannel();
		output.setBeanName("batchingProducer");
		binder.bindProducer("batching.0", output, properties);

		while (template.receive(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + "batching.0") != null) {
		}

		Log logger = spy(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor"))
				.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertEquals(Deflater.BEST_SPEED, TestUtils.getPropertyValue(binder, "binder.compressingPostProcessor.level"));

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Object out = spyOn("batching.0").receive(false);
		assertThat(out, instanceOf(byte[].class));
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar", new String((byte[]) out));

		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).trace(captor.capture());
		assertThat(captor.getValue().toString(), containsString("Compressed 14 to "));

		QueueChannel input = new QueueChannel();
		input.setBeanName("batchingConsumer");
		binder.bindConsumer("batching.0", input, null);

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Message<byte[]> in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("foo", new String(in.getPayload()));
		in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("bar", new String(in.getPayload()));
		assertNull(in.getHeaders().get(AmqpHeaders.DELIVERY_MODE));

		binder.unbindProducers("batching.0");
		binder.unbindConsumers("batching.0");
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

	private void verifyBarReplyConsumer(AbstractEndpoint endpoint) {
		List<?> replyMatchers;
		replyMatchers = TestUtils.getPropertyValue(endpoint,
				"headerMapper.replyHeaderMatcher.strategies",
				List.class);
		assertEquals(1, replyMatchers.size());
		assertEquals("bar",
				TestUtils.getPropertyValue(replyMatchers.get(0), "patterns", Collection.class).iterator().next());
	}

	private void verifyFooRequestBarReplyProducer(AbstractEndpoint endpoint) {
		verifyFooRequestProducer(endpoint);
		List<?> replyMatchers = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.replyHeaderMatcher.strategies",
				List.class);
		assertEquals(1, replyMatchers.size());
		assertEquals("bar",
				TestUtils.getPropertyValue(replyMatchers.get(0), "patterns", Collection.class).iterator().next());
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
					return template.receiveAndConvert(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + queue);
				}
				Object bar = null;
				int n = 0;
				while (n++ < 100 && bar == null) {
					bar = template.receiveAndConvert(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + queue);
					Thread.sleep(100);
				}
				assertTrue("Message did not arrive in RabbitMQ", n < 100);
				return bar;
			}

		};
	}

}
