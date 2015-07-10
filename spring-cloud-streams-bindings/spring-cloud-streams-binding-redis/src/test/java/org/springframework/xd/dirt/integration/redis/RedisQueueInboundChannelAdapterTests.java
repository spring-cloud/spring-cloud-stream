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
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.redis.inbound.RedisQueueMessageDrivenEndpoint;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.xd.dirt.integration.bus.BusTestUtils;
import org.springframework.xd.test.redis.RedisTestSupport;

/**
 * Integration test of {@link RedisQueueInboundChannelAdapter}
 *
 * @author Jennifer Hickey
 */
public class RedisQueueInboundChannelAdapterTests {

	private static final String QUEUE_NAME = "inboundadaptertest";

	private RedisConnectionFactory connectionFactory;

	private final BlockingDeque<Object> messages = new LinkedBlockingDeque<Object>(99);

	private RedisQueueMessageDrivenEndpoint adapter;

	@Rule
	public RedisTestSupport redisAvailableRule = new RedisTestSupport();

	private String currentQueueName;

	@Before
	public void setUp() {
		messages.clear();
		this.connectionFactory = redisAvailableRule.getResource();
		DirectChannel outputChannel = new DirectChannel();
		outputChannel.setBeanFactory(BusTestUtils.MOCK_BF);
		outputChannel.subscribe(new TestMessageHandler());

		this.currentQueueName = QUEUE_NAME + ":" + System.nanoTime();

		adapter = new RedisQueueMessageDrivenEndpoint(currentQueueName, connectionFactory);
		adapter.setBeanFactory(BusTestUtils.MOCK_BF);
		adapter.setOutputChannel(outputChannel);
	}

	@After
	public void tearDown() {
		adapter.stop();
		connectionFactory.getConnection().del(currentQueueName.getBytes());
	}

	@Test
	public void testDefaultPayloadSerializer() throws Exception {
		RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
		template.setConnectionFactory(connectionFactory);
		template.setKeySerializer(new StringRedisSerializer());
		template.afterPropertiesSet();

		adapter.afterPropertiesSet();
		adapter.start();

		template.boundListOps(currentQueueName).rightPush("message1");
		@SuppressWarnings("unchecked")
		Message<String> message = (Message<String>) messages.poll(1, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals("message1", message.getPayload());
	}

	@Test
	public void testDefaultMsgSerializer() throws Exception {
		RedisTemplate<String, Message<String>> template = new RedisTemplate<String, Message<String>>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new JdkSerializationRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setExpectMessage(true);
		adapter.afterPropertiesSet();
		adapter.start();

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("header1", "foo");
		template.boundListOps(currentQueueName).rightPush(new GenericMessage<String>("message2", headers));
		@SuppressWarnings("unchecked")
		Message<String> message = (Message<String>) messages.poll(1, TimeUnit.SECONDS);
		assertEquals("message2", message.getPayload());
		assertEquals("foo", message.getHeaders().get("header1"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNoSerializer() throws Exception {
		RedisTemplate<String, byte[]> template = new RedisTemplate<String, byte[]>();
		template.setEnableDefaultSerializer(false);
		template.setKeySerializer(new StringRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(null);
		adapter.afterPropertiesSet();
		adapter.start();

		template.boundListOps(currentQueueName).rightPush("message3".getBytes());
		Message<byte[]> message = (Message<byte[]>) messages.poll(1, TimeUnit.SECONDS);
		assertEquals("message3", new String(message.getPayload()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoSerializerNoExtractPayload() throws Exception {
		RedisTemplate<String, byte[]> template = new RedisTemplate<String, byte[]>();
		template.setEnableDefaultSerializer(false);
		template.setKeySerializer(new StringRedisSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(null);
		adapter.setExpectMessage(true);
		adapter.afterPropertiesSet();
		adapter.start();
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
		adapter.start();

		template.boundListOps(currentQueueName).rightPush(5l);
		@SuppressWarnings("unchecked")
		Message<Long> message = (Message<Long>) messages.poll(1, TimeUnit.SECONDS);
		assertEquals(5L, (long) message.getPayload());
	}

	@Test
	public void testCustomMessageSerializer() throws Exception {
		RedisTemplate<String, Message<?>> template = new RedisTemplate<String, Message<?>>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new TestMessageSerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		adapter.setSerializer(new TestMessageSerializer());
		adapter.setExpectMessage(true);
		adapter.afterPropertiesSet();
		adapter.start();

		template.boundListOps(currentQueueName).rightPush(new GenericMessage<Long>(10l));
		@SuppressWarnings("unchecked")
		Message<Long> message = (Message<Long>) messages.poll(1, TimeUnit.SECONDS);
		assertEquals(10L, (long) message.getPayload());
	}

	private class TestMessageHandler implements MessageHandler {

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			messages.add(message);
		}
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
