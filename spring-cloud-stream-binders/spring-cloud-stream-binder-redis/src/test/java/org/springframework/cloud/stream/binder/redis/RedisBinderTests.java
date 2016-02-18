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

package org.springframework.cloud.stream.binder.redis;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.EmbeddedHeadersMessageConverter;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.test.junit.redis.RedisTestSupport;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.expression.Expression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.redis.inbound.RedisQueueMessageDrivenEndpoint;
import org.springframework.integration.redis.outbound.RedisQueueOutboundChannelAdapter;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @author David Turanski
 * @author Mark Fisher
 */
public class RedisBinderTests extends PartitionCapableBinderTests {

	private final String CLASS_UNDER_TEST_NAME = RedisMessageChannelBinder.class.getSimpleName();

	@Rule
	public RedisTestSupport redisAvailableRule = new RedisTestSupport();

	private RedisTemplate<String, Object> redisTemplate;

	private static final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter =
			new EmbeddedHeadersMessageConverter();

	@Override
	protected Binder<MessageChannel> getBinder() {
		if (testBinder == null) {
			testBinder = new RedisTestBinder(redisAvailableRule.getResource());
		}
		return testBinder;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Test
	public void testConsumerProperties() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("maxAttempts", "1"); // disable retry
		Binding<MessageChannel> binding = binder.bindConsumer("props.0", "test", new DirectChannel(), properties);
		assertEquals(binding, binding);
		AbstractEndpoint endpoint = extractEndpoint(binding);
		assertThat(endpoint, instanceOf(RedisQueueMessageDrivenEndpoint.class));
		assertSame(DirectChannel.class, TestUtils.getPropertyValue(endpoint, "outputChannel").getClass());
		binding.unbind();
		assertFalse(extractEndpoint(binding).isRunning());

		properties.put("backOffInitialInterval", "2000");
		properties.put("backOffMaxInterval", "20000");
		properties.put("backOffMultiplier", "5.0");
		properties.put("concurrency", "2");
		properties.put("maxAttempts", "23");
		properties.put("partitionIndex", 0);

		binding = binder.bindConsumer("props.0", "test", new DirectChannel(), properties);
		endpoint = extractEndpoint(binding);
		verifyConsumer(endpoint);

		binding.unbind();
		assertFalse(extractEndpoint(binding).isRunning());
	}

	@Test
	public void testProducerProperties() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("props.0", "test", new DirectChannel(), null);
		Binding<MessageChannel> producerBinding = binder.bindProducer("props.0", new DirectChannel(), null);
		AbstractEndpoint endpoint = extractEndpoint(producerBinding);
		@SuppressWarnings("unchecked")
		Map<String, RedisQueueOutboundChannelAdapter> adapters = TestUtils.getPropertyValue(endpoint, "handler.adapters", Map.class);
		RedisQueueOutboundChannelAdapter adapter = adapters.get("test");
		assertEquals(
				"props.0.test",
				TestUtils.getPropertyValue(adapter, "queueNameExpression", Expression.class).getExpressionString());
		producerBinding.unbind();
		assertFalse(endpoint.isRunning());

		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "'foo'");
		properties.put("partitionKeyExtractorClass", "foo");
		properties.put("partitionSelectorExpression", "0");
		properties.put("partitionSelectorClass", "foo");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "1");

		producerBinding = binder.bindProducer("props.0", new DirectChannel(), properties);
		endpoint = extractEndpoint(producerBinding);
		adapter = (RedisQueueOutboundChannelAdapter) TestUtils.getPropertyValue(endpoint, "handler.adapters", Map.class).get("test");
		assertEquals(
				"'props.0.test-' + headers['partition']",
				TestUtils.getPropertyValue(adapter, "queueNameExpression", Expression.class).getExpressionString());

		producerBinding.unbind();
		consumerBinding.unbind();
		assertFalse(extractEndpoint(producerBinding).isRunning());
		assertFalse(extractEndpoint(consumerBinding).isRunning());
	}

	private void verifyConsumer(AbstractEndpoint endpoint) {
		assertThat(endpoint.getClass().getName(), containsString("CompositeRedisQueueMessageDrivenEndpoint"));
		assertEquals(2, TestUtils.getPropertyValue(endpoint, "consumers", Collection.class).size());
		DirectChannel channel = TestUtils.getPropertyValue(
				TestUtils.getPropertyValue(endpoint, "consumers", List.class).get(0),
				"outputChannel", DirectChannel.class);
		assertThat(
				channel.getClass().getName(), containsString(getClassUnderTestName() + "$")); // retry wrapper
		assertThat(
				TestUtils.getPropertyValue(TestUtils.getPropertyValue(endpoint, "consumers", List.class).get(1),
						"outputChannel").getClass().getName(), containsString(getClassUnderTestName() + "$")); // retry wrapper
		RetryTemplate retry = TestUtils.getPropertyValue(channel, "val$retryTemplate", RetryTemplate.class);
		assertEquals(23, TestUtils.getPropertyValue(retry, "retryPolicy.maxAttempts"));
		assertEquals(2000L, TestUtils.getPropertyValue(retry, "backOffPolicy.initialInterval"));
		assertEquals(20000L, TestUtils.getPropertyValue(retry, "backOffPolicy.maxInterval"));
		assertEquals(5.0, TestUtils.getPropertyValue(retry, "backOffPolicy.multiplier"));
	}

	@Test
	public void testRetryFail() {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel channel = new DirectChannel();
		binder.bindProducer("retry.0", channel, null);
		Properties props = new Properties();
		props.put("maxAttempts", 2);
		props.put("backOffInitialInterval", 100);
		props.put("backOffMultiplier", "1.0");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retry.0", "test", new DirectChannel(), props); // no subscriber
		channel.send(new GenericMessage<String>("foo"));
		RedisTemplate<String, Object> template = createTemplate();
		Object rightPop = template.boundListOps("ERRORS:retry.0.test").rightPop(5, TimeUnit.SECONDS);
		assertNotNull(rightPop);
		assertThat(new String((byte[]) rightPop), containsString("foo"));
		consumerBinding.unbind();
	}

	@Test
	public void testMoreHeaders() {
		RedisMessageChannelBinder binder = new RedisMessageChannelBinder(mock(RedisConnectionFactory.class), "foo", "bar");
		Collection<String> headers = Arrays.asList(TestUtils.getPropertyValue(binder, "headersToMap", String[].class));
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
	@SuppressWarnings("unchecked")
	protected String getEndpointRouting(AbstractEndpoint endpoint) {
		Map<String, RedisQueueOutboundChannelAdapter> adapters = TestUtils.getPropertyValue(endpoint, "handler.adapters", Map.class);
		return TestUtils.getPropertyValue(adapters.values().iterator().next(), "queueNameExpression", Expression.class).getExpressionString();
	}

	@Override
	protected String getExpectedRoutingBaseDestination(String name, String group) {
		return name + "." + group;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(final String queue) {
		final RedisTemplate<String, Object> template = createTemplate();
		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				byte[] bytes = (byte[]) template.boundListOps(queue).rightPop(50, TimeUnit.MILLISECONDS);
				if (bytes == null) {
					return null;
				}
				bytes = (byte[]) embeddedHeadersMessageConverter.extractHeaders(new GenericMessage<byte[]>(bytes), false).getPayload();
				return new String(bytes, "UTF-8");
			}

		};
	}

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(3000); // needed for Redis see INT-3442
	}

}
