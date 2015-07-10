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

package org.springframework.xd.dirt.integration.bus.redis;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.expression.Expression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.redis.inbound.RedisQueueMessageDrivenEndpoint;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.EmbeddedHeadersMessageConverter;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.PartitionCapableBusTests;
import org.springframework.xd.dirt.integration.bus.Spy;
import org.springframework.xd.dirt.integration.redis.RedisMessageBus;
import org.springframework.xd.test.redis.RedisTestSupport;

/**
 * @author Gary Russell
 */
public class RedisMessageBusTests extends PartitionCapableBusTests {

	@Rule
	public RedisTestSupport redisAvailableRule = new RedisTestSupport();

	private RedisTemplate<String, Object> redisTemplate;

	private static final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter =
			new EmbeddedHeadersMessageConverter();

	@Override
	protected MessageBus getMessageBus() {
		if (testMessageBus == null) {
			testMessageBus = new RedisTestMessageBus(redisAvailableRule.getResource(), getCodec());
		}
		return testMessageBus;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Override
	public void testSendAndReceivePubSub() throws Exception {

		TimeUnit.SECONDS.sleep(2); //TODO remove timing issue

		super.testSendAndReceivePubSub();
	}

	@Before
	public void setup() {
		createTemplate().boundListOps("queue.direct.0").trim(1, 0);
	}

	@Test
	public void testConsumerProperties() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("maxAttempts", "1"); // disable retry
		bus.bindConsumer("props.0", new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		assertThat(endpoint, instanceOf(RedisQueueMessageDrivenEndpoint.class));
		assertSame(DirectChannel.class, TestUtils.getPropertyValue(endpoint, "outputChannel").getClass());
		bus.unbindConsumers("props.0");
		assertEquals(0, bindings.size());

		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");
		properties.put("partitionIndex", 0);

		bus.bindConsumer("props.0", new DirectChannel(), properties);
		assertEquals(1, bindings.size());
		endpoint = bindings.get(0).getEndpoint();
		verifyConsumer(endpoint);

		try {
			bus.bindPubSubConsumer("dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RedisMessageBus does not support consumer properties: "),
					containsString("partitionIndex"),
					containsString("concurrency"),
					containsString(" for dummy.")));
		}
		try {
			bus.bindConsumer("queue:dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertEquals("RedisMessageBus does not support consumer property: partitionIndex for queue:dummy.",
					e.getMessage());
		}

		bus.unbindConsumers("props.0");
		assertEquals(0, bindings.size());
	}

	@Test
	public void testProducerProperties() throws Exception {
		MessageBus bus = getMessageBus();
		bus.bindProducer("props.0", new DirectChannel(), null);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
		assertEquals(
				"queue.props.0",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.queueNameExpression", Expression.class).getExpressionString());
		bus.unbindProducers("props.0");
		assertEquals(0, bindings.size());

		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "1");

		bus.bindProducer("props.0", new DirectChannel(), properties);
		assertEquals(1, bindings.size());
		endpoint = bindings.get(0).getEndpoint();
		assertEquals(
				"'queue.props.0-' + headers['partition']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.queueNameExpression", Expression.class).getExpressionString());

		try {
			bus.bindPubSubProducer("dummy", null, properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RedisMessageBus does not support producer properties: "),
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
					containsString("RedisMessageBus does not support producer properties: "),
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

		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");

		bus.bindRequestor("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);

		assertEquals(2, bindings.size());
		AbstractEndpoint endpoint = bindings.get(0).getEndpoint(); // producer
		assertEquals(
				"queue.props.0.requests",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.queueNameExpression", Expression.class).getExpressionString());

		endpoint = bindings.get(1).getEndpoint(); // consumer
		verifyConsumer(endpoint);

		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put("partitionIndex", "0");
		try {
			bus.bindRequestor("dummy", new DirectChannel(), new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RedisMessageBus does not support producer properties: "),
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

		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");

		bus.bindReplier("props.0", new DirectChannel(), new DirectChannel(), properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);

		assertEquals(2, bindings.size());
		AbstractEndpoint endpoint = bindings.get(1).getEndpoint(); // producer
		assertEquals(
				"headers['replyTo']",
				TestUtils.getPropertyValue(endpoint, "handler.delegate.queueNameExpression", Expression.class).getExpressionString());

		endpoint = bindings.get(0).getEndpoint(); // consumer
		verifyConsumer(endpoint);

		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "1");
		properties.put("partitionIndex", "0");
		try {
			bus.bindReplier("dummy", new DirectChannel(), new DirectChannel(), properties);
			fail("Expected exception");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(
					containsString("RedisMessageBus does not support consumer properties: "),
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

	private void verifyConsumer(AbstractEndpoint endpoint) {
		assertThat(endpoint.getClass().getName(), containsString("CompositeRedisQueueMessageDrivenEndpoint"));
		assertEquals(2, TestUtils.getPropertyValue(endpoint, "consumers", Collection.class).size());
		DirectChannel channel = TestUtils.getPropertyValue(
				TestUtils.getPropertyValue(endpoint, "consumers", List.class).get(0),
				"outputChannel", DirectChannel.class);
		assertThat(
				channel.getClass().getName(), containsString("RedisMessageBus$")); // retry wrapper
		assertThat(
				TestUtils.getPropertyValue(TestUtils.getPropertyValue(endpoint, "consumers", List.class).get(1),
						"outputChannel").getClass().getName(), containsString("RedisMessageBus$")); // retry wrapper
		RetryTemplate retry = TestUtils.getPropertyValue(channel, "val$retryTemplate", RetryTemplate.class);
		assertEquals(23, TestUtils.getPropertyValue(retry, "retryPolicy.maxAttempts"));
		assertEquals(2000L, TestUtils.getPropertyValue(retry, "backOffPolicy.initialInterval"));
		assertEquals(20000L, TestUtils.getPropertyValue(retry, "backOffPolicy.maxInterval"));
		assertEquals(5.0, TestUtils.getPropertyValue(retry, "backOffPolicy.multiplier"));
	}

	@Test
	public void testRetryFail() {
		MessageBus bus = getMessageBus();
		DirectChannel channel = new DirectChannel();
		bus.bindProducer("retry.0", channel, null);
		Properties props = new Properties();
		props.put("maxAttempts", 2);
		props.put("backOffInitialInterval", 100);
		props.put("backOffMultiplier", "1.0");
		bus.bindConsumer("retry.0", new DirectChannel(), props); // no subscriber
		channel.send(new GenericMessage<String>("foo"));
		RedisTemplate<String, Object> template = createTemplate();
		Object rightPop = template.boundListOps("ERRORS:retry.0").rightPop(5, TimeUnit.SECONDS);
		assertNotNull(rightPop);
		assertThat(new String((byte[]) rightPop), containsString("foo"));
	}

	@Test
	public void testMoreHeaders() {
		RedisMessageBus bus = new RedisMessageBus(mock(RedisConnectionFactory.class), getCodec(), "foo", "bar");
		Collection<String> headers = Arrays.asList(TestUtils.getPropertyValue(bus, "headersToMap", String[].class));
		assertEquals(10, headers.size());
		assertTrue(headers.contains("foo"));
		assertTrue(headers.contains("bar"));
	}

	private RedisTemplate<String, Object> createTemplate() {
		if (this.redisTemplate != null) {
			return this.redisTemplate;
		}
		RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
		template.setConnectionFactory(this.redisAvailableRule.getResource());
		template.setKeySerializer(new StringRedisSerializer());
		template.setEnableDefaultSerializer(false);
		template.afterPropertiesSet();
		this.redisTemplate = template;
		return template;
	}

	@Override
	protected String getEndpointRouting(AbstractEndpoint endpoint) {
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.queueNameExpression", Expression.class).getExpressionString();
	}

	@Override
	protected String getPubSubEndpointRouting(AbstractEndpoint endpoint) {
		return TestUtils.getPropertyValue(endpoint, "handler.delegate.topicExpression", Expression.class).getExpressionString();
	}

	@Override
	public Spy spyOn(final String queue) {
		final RedisTemplate<String, Object> template = createTemplate();
		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				byte[] bytes = (byte[]) template.boundListOps("queue." + queue).rightPop(50, TimeUnit.MILLISECONDS);
				if (bytes == null) {
					return null;
				}
				bytes = (byte[]) embeddedHeadersMessageConverter.extractHeaders(new GenericMessage<byte[]>(bytes), false).getPayload();
				return new String(bytes, "UTF-8");
			}

		};
	}

	@Override
	protected void busBindUnbindLatency() throws InterruptedException {
		Thread.sleep(3000); // needed for Redis see INT-3442
	}

}
