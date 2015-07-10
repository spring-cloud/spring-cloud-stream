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

package org.springframework.xd.dirt.integration.bus.rabbit;

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
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.PartitionCapableBusTests;
import org.springframework.xd.dirt.integration.bus.Spy;
import org.springframework.xd.test.rabbit.RabbitTestSupport;

/**
 * @author Mark Fisher
 * @author Gary Russell
 */
public class RabbitMessageBusTests extends PartitionCapableBusTests {

	@Rule
	public RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport();

	@Override
	protected MessageBus getMessageBus() {
		if (testMessageBus == null) {
			testMessageBus = new RabbitTestMessageBus(rabbitAvailableRule.getResource(), getCodec());
		}
		return testMessageBus;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testSendAndReceiveBad() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		messageBus.bindProducer("bad.0", moduleOutputChannel, null);
		messageBus.bindConsumer("bad.0", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("bad").setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
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
		messageBus.unbindConsumers("bad.0");
		messageBus.unbindProducers("bad.0");
	}

	@Test
	public void testConsumerProperties() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("transacted", "true"); // test transacted with defaults; not allowed with ackmode NONE
		bus.bindConsumer("props.0", new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.AUTO, container.getAcknowledgeMode());
		assertEquals("xdbus.props.0", container.getQueueNames()[0]);
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
		bus.unbindConsumers("props.0");
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
		bus.bindConsumer("props.0", new DirectChannel(), properties);

		@SuppressWarnings("unchecked")
		List<Binding> bindingsNow = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindingsNow.size());
		endpoint = bindingsNow.get(0).getEndpoint();
		container = verifyContainer(endpoint);

		assertEquals("foo.props.0", container.getQueueNames()[0]);

		try {
			bus.bindPubSubConsumer("dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RabbitMessageBus does not support consumer properties: "),
					containsString("partitionIndex"),
					containsString("concurrency"),
					containsString(" for dummy.")));
		}
		try {
			bus.bindConsumer("queue:dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertEquals("RabbitMessageBus does not support consumer property: partitionIndex for queue:dummy.",
					e.getMessage());
		}

		bus.unbindConsumers("props.0");
		assertEquals(0, bindingsNow.size());
	}

	@Test
	public void testProducerProperties() throws Exception {
		MessageBus bus = getMessageBus();
		bus.bindProducer("props.0", new DirectChannel(), null);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		assertEquals("xdbus.props.0", TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKey"));
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.PERSISTENT, mode);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.requestHeaderMatcher.strategies", List.class);
		assertEquals(2, requestHeaders.size());
		bus.unbindProducers("props.0");
		assertEquals(0, bindings.size());

		Properties properties = new Properties();
		properties.put("prefix", "foo.");
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("requestHeaderPatterns", "foo");
		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "1");

		bus.bindProducer("props.0", new DirectChannel(), properties);
		assertEquals(1, bindings.size());
		endpoint = bindings.get(0).getEndpoint();
		assertEquals(
				"'foo.props.0-' + headers['partition']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression", SpelExpression.class).getExpressionString());
		mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);
		verifyFooRequestProducer(endpoint);

		try {
			bus.bindPubSubProducer("dummy", new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RabbitMessageBus does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), containsString("for dummy."));
		}
		try {
			bus.bindProducer("queue:dummy", new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RabbitMessageBus does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), containsString("for queue:dummy."));
		}

		bus.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testRequestReplyRequestorProperties() throws Exception {
		MessageBus bus = getMessageBus();
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

		bus.bindRequestor("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);

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
		properties.put(BusProperties.NEXT_MODULE_COUNT, "1");
		properties.put("partitionIndex", "0");
		try {
			bus.bindRequestor("dummy", null, null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RabbitMessageBus does not support producer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), allOf(containsString("partitionIndex"), containsString("for dummy.")));
		}

		bus.unbindConsumers("props.0");
		bus.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testRequestReplyReplierProperties() throws Exception {
		MessageBus bus = getMessageBus();
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

		bus.bindReplier("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);

		assertEquals(2, bindings.size());
		AbstractEndpoint endpoint = bindings.get(1).getEndpoint(); // producer
		assertEquals(
				"headers['amqp_replyTo']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression", SpelExpression.class).getExpressionString());
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
		properties.put(BusProperties.NEXT_MODULE_COUNT, "1");
		properties.put("partitionIndex", "0");
		try {
			bus.bindReplier("dummy", null, null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RabbitMessageBus does not support consumer properties: "),
					containsString("partitionSelectorExpression"),
					containsString("partitionKeyExtractorClass"),
					containsString("partitionKeyExpression"),
					containsString("partitionSelectorClass")));
			assertThat(e.getMessage(), allOf(containsString("partitionIndex"), containsString("for dummy.")));
		}

		bus.unbindConsumers("props.0");
		bus.unbindProducers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("prefix", "xdbustest.");
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
		bus.bindPubSubConsumer("teststream.tap:stream:durabletest.0", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("xdbustest.topic.tap:stream:durabletest.0", "", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert("xdbustest.teststream.tap:stream:durabletest.0.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		bus.unbindConsumer("teststream.tap:stream:durabletest.0", moduleInputChannel);
		assertNotNull(admin.getQueueProperties("xdbustest.teststream.tap:stream:durabletest.0.dlq"));
		admin.deleteQueue("xdbustest.teststream.tap:stream:durabletest.0.dlq");
		admin.deleteQueue("xdbustest.teststream.tap:stream:durabletest.0");
		admin.deleteExchange("xdbustest.topic.tap:stream:durabletest.0");
		admin.deleteExchange("xdbustest.DLX");
	}

	@Test
	public void testNonDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("prefix", "xdbustest.");
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
		bus.bindPubSubConsumer("teststream.tap:stream:nondurabletest.0", moduleInputChannel, properties);

		bus.unbindConsumer("teststream.tap:stream:nondurabletest.0", moduleInputChannel);
		assertNull(admin.getQueueProperties("xdbustest.teststream.tap:stream:nondurabletest.0.dlq"));
		admin.deleteQueue("xdbustest.teststream.tap:stream:nondurabletest.0");
		admin.deleteExchange("xdbustest.topic.tap:stream:nondurabletest.0");
	}

	@Test
	public void testAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("prefix", "xdbustest.");
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
		bus.bindConsumer("dlqtest", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", "xdbustest.dlqtest", "foo");

		int n = 0;
		while (n++ < 100) {
			Object deadLetter = template.receiveAndConvert("xdbustest.dlqtest.dlq");
			if (deadLetter != null) {
				assertEquals("foo", deadLetter);
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		bus.unbindConsumer("dlqtest", moduleInputChannel);
		admin.deleteQueue("xdbustest.dlqtest.dlq");
		admin.deleteQueue("xdbustest.dlqtest");
		admin.deleteExchange("xdbustest.DLX");
	}

	@Test
	public void testAutoBindDLQwithRepublish() throws Exception {
		// pre-declare the queue with dead-lettering, users can also use a policy
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", "xdbustest.DLX");
		Queue queue = new Queue("xdbustest.dlqpubtest", true, false, false, args);
		admin.declareQueue(queue);

		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("prefix", "xdbustest.");
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
		bus.bindConsumer("dlqpubtest", moduleInputChannel, properties);

		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.convertAndSend("", "xdbustest.dlqpubtest", "foo");

		int n = 0;
		while (n++ < 100) {
			org.springframework.amqp.core.Message deadLetter = template.receive("xdbustest.dlqpubtest.dlq");
			if (deadLetter != null) {
				assertEquals("foo", new String(deadLetter.getBody()));
				assertNotNull(deadLetter.getMessageProperties().getHeaders().get("x-exception-stacktrace"));
				break;
			}
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		bus.unbindConsumer("dlqpubtest", moduleInputChannel);
		admin.deleteQueue("xdbustest.dlqpubtest.dlq");
		admin.deleteQueue("xdbustest.dlqpubtest");
		admin.deleteExchange("xdbustest.DLX");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchingAndCompression() throws Exception {
		RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("batchingEnabled", "true");
		properties.put("batchSize", "2");
		properties.put("batchBufferLimit", "100000");
		properties.put("batchTimeout", "30000");
		properties.put("compress", "true");

		DirectChannel output = new DirectChannel();
		output.setBeanName("batchingProducer");
		bus.bindProducer("batching.0", output, properties);

		while (template.receive("xdbus.batching.0") != null) {
		}

		Log logger = spy(TestUtils.getPropertyValue(bus, "messageBus.compressingPostProcessor.logger", Log.class));
		new DirectFieldAccessor(TestUtils.getPropertyValue(bus, "messageBus.compressingPostProcessor"))
				.setPropertyValue("logger", logger);
		when(logger.isTraceEnabled()).thenReturn(true);

		assertEquals(Deflater.BEST_SPEED, TestUtils.getPropertyValue(bus, "messageBus.compressingPostProcessor.level"));

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
		bus.bindConsumer("batching.0", input, null);

		output.send(new GenericMessage<>("foo".getBytes()));
		output.send(new GenericMessage<>("bar".getBytes()));

		Message<byte[]> in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("foo", new String(in.getPayload()));
		in = (Message<byte[]>) input.receive(10000);
		assertNotNull(in);
		assertEquals("bar", new String(in.getPayload()));
		assertNull(in.getHeaders().get(AmqpHeaders.DELIVERY_MODE));

		bus.unbindProducers("batching.0");
		bus.unbindConsumers("batching.0");
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
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression", SpelExpression.class).getExpressionString();
	}

	@Override
	protected String getPubSubEndpointRouting(AbstractEndpoint endpoint) {
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.exchangeNameExpression", SpelExpression.class).getExpressionString();
	}

	@Override
	public Spy spyOn(final String queue) {
		final RabbitTemplate template = new RabbitTemplate(this.rabbitAvailableRule.getResource());
		template.setAfterReceivePostProcessor(new DelegatingDecompressingPostProcessor());
		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				if (expectNull) {
					Thread.sleep(50);
					return template.receiveAndConvert("xdbus." + queue);
				}
				Object bar = null;
				int n = 0;
				while (n++ < 100 && bar == null) {
					bar = template.receiveAndConvert("xdbus." + queue);
					Thread.sleep(100);
				}
				assertTrue("Message did not arrive in RabbitMQ", n < 100);
				return bar;
			}

		};
	}

}
