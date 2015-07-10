/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.redis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.integration.redis.outbound.RedisQueueOutboundChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.xd.dirt.integration.bus.BusTestUtils;
import org.springframework.xd.test.redis.RedisTestSupport;

/**
 * Integration test of {@link RedisQueueOutboundChannelAdapter}
 *
 * @author Jennifer Hickey
 * @author Gary Russell
 */
public class RedisQueueOutboundChannelAdapterTests {

	private static final String QUEUE_NAME = "outboundadaptertest";

	private RedisConnectionFactory connectionFactory;

	private RedisQueueOutboundChannelAdapter adapter;

	@Rule
	public RedisTestSupport redisAvailableRule = new RedisTestSupport();

	@Before
	public void setUp() {
		this.connectionFactory = redisAvailableRule.getResource();
		adapter = new RedisQueueOutboundChannelAdapter(QUEUE_NAME, connectionFactory);
		adapter.setBeanFactory(BusTestUtils.MOCK_BF);
	}

	@After
	public void tearDown() {
		connectionFactory.getConnection().del(QUEUE_NAME.getBytes());
	}

	@Test
	public void testDefaultPayloadSerializer() throws Exception {
		StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		adapter.afterPropertiesSet();
		adapter.handleMessage(new GenericMessage<String>("message1"));
		assertEquals("message1", template.boundListOps(QUEUE_NAME).rightPop());
	}

	@Test
	public void testDefaultMsgSerializer() throws Exception {
		RedisTemplate<String, Message<?>> template = new RedisTemplate<String, Message<?>>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new JdkSerializationRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setExtractPayload(false);
		adapter.afterPropertiesSet();

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("header1", "foo");
		adapter.handleMessage(new GenericMessage<String>("message2", headers));
		Message<?> message = template.boundListOps(QUEUE_NAME).rightPop();
		assertEquals("message2", message.getPayload());
		assertEquals("foo", message.getHeaders().get("header1"));
	}

	@Test
	public void testNoSerializer() throws Exception {
		RedisTemplate<String, byte[]> template = new RedisTemplate<String, byte[]>();
		template.setEnableDefaultSerializer(false);
		template.setKeySerializer(new StringRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.afterPropertiesSet();

		adapter.handleMessage(new GenericMessage<byte[]>("message3".getBytes()));
		byte[] value = template.boundListOps(QUEUE_NAME).rightPop();
		assertEquals("message3", new String(value));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoSerializerNoExtractPayload() throws Exception {
		RedisTemplate<String, byte[]> template = new RedisTemplate<String, byte[]>();
		template.setEnableDefaultSerializer(false);
		template.setKeySerializer(new StringRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(null);
		adapter.setExtractPayload(false);
		adapter.afterPropertiesSet();
	}

	@Test
	public void testCustomPayloadSerializer() throws Exception {
		RedisTemplate<String, Long> template = new RedisTemplate<String, Long>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(new GenericToStringSerializer<Long>(Long.class));
		adapter.afterPropertiesSet();

		adapter.handleMessage(new GenericMessage<Long>(5l));
		assertEquals(Long.valueOf(5), template.boundListOps(QUEUE_NAME).rightPop());
	}

	@Test
	public void testCustomMessageSerializer() throws Exception {
		RedisTemplate<String, Message<?>> template = new RedisTemplate<String, Message<?>>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new TestMessageSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(new TestMessageSerializer());
		adapter.setExtractPayload(false);
		adapter.afterPropertiesSet();

		Message<?> message = template.boundListOps(QUEUE_NAME).rightPop();
		assertEquals(10l, message.getPayload());
	}

	private class TestMessageSerializer implements RedisSerializer<Message<?>> {

		@Override
		public byte[] serialize(Message<?> t) throws SerializationException {
			return "Foo".getBytes();
		}

		@Override
		public Message<?> deserialize(byte[] bytes) throws SerializationException {
			return new GenericMessage<Long>(10l);
		}
	}
}
