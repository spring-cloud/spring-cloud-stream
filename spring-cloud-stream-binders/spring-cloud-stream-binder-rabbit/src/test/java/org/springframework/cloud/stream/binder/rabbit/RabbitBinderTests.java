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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
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
public class RabbitBinderTests extends PartitionCapableBinderTests {

	private final String CLASS_UNDER_TEST_NAME = RabbitMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Rule
	public RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport();

	@Override
	protected Binder<MessageChannel> getBinder() {
		if (testBinder == null) {
			testBinder = new RabbitTestBinder(rabbitAvailableRule.getResource());
		}
		return testBinder;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testSendAndReceiveBad() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0", moduleOutputChannel, null);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test", moduleInputChannel, null);
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
		consumerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerProperties() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("transacted", "true"); // test transacted with defaults; not allowed with ackmode NONE
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("props.0", null, new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = ((DefaultBinding)bindings.get(0)).getEndpoint();
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(endpoint, "messageListenerContainer",
				SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.AUTO, container.getAcknowledgeMode());
		assertThat(container.getQueueNames()[0],
				startsWith(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX));
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
		consumerBinding = binder.bindConsumer("props.0", "test", new DirectChannel(), properties);

		@SuppressWarnings("unchecked")
		List<DefaultBinding<MessageChannel>> bindingsNow = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindingsNow.size());
		endpoint = bindingsNow.get(0).getEndpoint();
		container = verifyContainer(endpoint);

		assertEquals("foo.props.0.test", container.getQueueNames()[0]);

		consumerBinding.unbind();
		assertEquals(0, bindingsNow.size());
	}

	@Test
	public void testProducerProperties() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Binding<MessageChannel> producerBinding = binder.bindProducer("props.0", new DirectChannel(), null);
		@SuppressWarnings("unchecked")
		List<DefaultBinding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		MessageDeliveryMode mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.PERSISTENT, mode);
		List<?> requestHeaders = TestUtils.getPropertyValue(endpoint,
				"handler.delegate.headerMapper.requestHeaderMatcher.strategies", List.class);
		assertEquals(2, requestHeaders.size());
		producerBinding.unbind();
		assertEquals(0, bindings.size());

		Properties properties = new Properties();
		properties.put("prefix", "foo.");
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("requestHeaderPatterns", "foo");
		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "1");

		producerBinding = binder.bindProducer("props.0", new DirectChannel(), properties);
		assertEquals(1, bindings.size());
		endpoint = bindings.get(0).getEndpoint();
		assertEquals(
				"'props.0-' + headers['partition']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.routingKeyExpression",
						SpelExpression.class).getExpressionString());
		mode = TestUtils.getPropertyValue(endpoint, "handler.delegate.defaultDeliveryMode",
				MessageDeliveryMode.class);
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, mode);
		verifyFooRequestProducer(endpoint);

		producerBinding.unbind();
		assertEquals(0, bindings.size());
	}

	@Test
	public void testDurablePubSubWithAutoBindDLQ() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.rabbitAvailableRule.getResource());

		Binder<MessageChannel> binder = getBinder();

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

		Binder<MessageChannel> binder = getBinder();
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
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("nondurabletest.0", "tgroup", moduleInputChannel, properties);

		consumerBinding.unbind();
		assertNull(admin.getQueueProperties(TEST_PREFIX + "nondurabletest.0.dlq"));
	}

	@Test
	public void testAutoBindDLQ() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		properties.put("durableSubscription","true");
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
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", "bindertest.");
		properties.put("autoBindDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		properties.put("partitionIndex", "0");
		properties.put("durableSubscription","true");
		DirectChannel input0 = new DirectChannel();
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 =
				binder.bindConsumer("partDLQ.0", "default", new QueueChannel(), properties);
		properties.put("partitionIndex", "1");
		DirectChannel input1 = new DirectChannel();
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.0", "dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.0", "default", new QueueChannel(), properties);

		properties.clear();
		properties.put("prefix", "bindertest.");
		properties.put("autoBindDLQ", "true");
		properties.put("partitionKeyExtractorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "2");
		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.0", output, properties);

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
	public void testAutoBindDLQPartionedProducerFirst() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();

		properties.put("prefix", "bindertest.");
		properties.put("autoBindDLQ", "true");
		properties.put("partitionKeyExtractorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "2");
		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partDLQ.1", output, properties);

		properties.clear();
		properties.put("prefix", "bindertest.");
		properties.put("autoBindDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		properties.put("partitionIndex", "0");
		properties.put(BinderPropertyKeys.DURABLE,"true");
		DirectChannel input0 = new DirectChannel();
		input0.setBeanName("test.input0DLQ");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input0, properties);
		Binding<MessageChannel> defaultConsumerBinding1 = binder.bindConsumer("partDLQ.1", "defaultConsumer", new QueueChannel(), properties);
		properties.put("partitionIndex", "1");
		DirectChannel input1 = new DirectChannel();
		input1.setBeanName("test.input1DLQ");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partDLQ.1", "dlqPartGrp", input1, properties);
		Binding<MessageChannel> defaultConsumerBinding2 = binder.bindConsumer("partDLQ.1", "defaultConsumer", new QueueChannel(), properties);

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

		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("prefix", TEST_PREFIX);
		properties.put("autoBindDLQ", "true");
		properties.put("republishToDLQ", "true");
		properties.put("maxAttempts", "1"); // disable retry
		properties.put("requeue", "false");
		properties.put("durableSubscription", "true");
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
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("deliveryMode", "NON_PERSISTENT");
		properties.put("batchingEnabled", "true");
		properties.put("batchSize", "2");
		properties.put("batchBufferLimit", "100000");
		properties.put("batchTimeout", "30000");
		properties.put("compress", "true");

		DirectChannel output = new DirectChannel();
		output.setBeanName("batchingProducer");
		Binding<MessageChannel> producerBinding = binder.bindProducer("batching.0", output, properties);

		while (template.receive(RabbitMessageChannelBinder.DEFAULT_RABBIT_PREFIX + "batching.0.default") != null) {
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
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("batching.0", "test", input, null);

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
		rabbitBinder.setDefaultAutoBindDLQ(true);
		AbstractTestBinder<RabbitMessageChannelBinder> binder = new RabbitTestBinder(cf, rabbitBinder);

		Properties properties = new Properties();
		properties.put("prefix", "latebinder.");

		MessageChannel moduleOutputChannel = new DirectChannel();
		Binding<MessageChannel> late0ProducerBinding = binder.bindProducer("late.0", moduleOutputChannel, properties);

		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> late0ConsumerBinding = binder.bindConsumer("late.0", "test", moduleInputChannel, properties);

		properties.put("partitionKeyExpression", "payload.equals('0') ? 0 : 1");
		properties.put("partitionSelectorExpression", "hashCode()");
		properties.put("nextModuleCount", "2");

		MessageChannel partOutputChannel = new DirectChannel();
		Binding<MessageChannel> partlate0ProducerBinding = binder.bindProducer("partlate.0", partOutputChannel, properties);

		QueueChannel partInputChannel0 = new QueueChannel();
		QueueChannel partInputChannel1 = new QueueChannel();
		properties.clear();
		properties.put("prefix", "latebinder.");
		properties.put("partitionIndex", "0");
		Binding<MessageChannel> partlate0Consumer0Binding = binder.bindConsumer("partlate.0", "test", partInputChannel0, properties);
		properties.put("partitionIndex", "1");
		Binding<MessageChannel> partlate0Consumer1Binding = binder.bindConsumer("partlate.0", "test", partInputChannel1, properties);

		rabbitBinder.setDefaultAutoBindDLQ(false);
		properties.clear();
		properties.put("prefix", "latebinder.");
		MessageChannel noDLQOutputChannel = new DirectChannel();
		Binding<MessageChannel> noDlqProducerBinding = binder.bindProducer("lateNoDLQ.0", noDLQOutputChannel, properties);

		QueueChannel noDLQInputChannel = new QueueChannel();
		Binding<MessageChannel> noDlqConsumerBinding = binder.bindConsumer("lateNoDLQ.0", "test", noDLQInputChannel, properties);

		MessageChannel outputChannel = new DirectChannel();
		Binding<MessageChannel> pubSubProducerBinding = binder.bindProducer("latePubSub", outputChannel, properties);
		QueueChannel pubSubInputChannel = new QueueChannel();
		Binding<MessageChannel> nonDurableConsumerBinding = binder.bindConsumer("latePubSub", "lategroup", pubSubInputChannel, properties);
		QueueChannel durablePubSubInputChannel = new QueueChannel();
		properties.setProperty("durableSubscription", "true");
		Binding<MessageChannel> durableConsumerBinding = binder.bindConsumer("latePubSub", "lateDurableGroup", durablePubSubInputChannel, properties);

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
