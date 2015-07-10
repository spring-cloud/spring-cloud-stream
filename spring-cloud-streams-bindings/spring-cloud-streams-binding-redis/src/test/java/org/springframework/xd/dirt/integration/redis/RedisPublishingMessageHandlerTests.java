/*
 * Copyright 2013 the original author or authors.
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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.integration.redis.outbound.RedisPublishingMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.xd.dirt.integration.bus.BusTestUtils;
import org.springframework.xd.test.redis.RedisTestSupport;

/**
 * Temporary copy of SI RedisPublishingMessageHandlerTests that adds tests that publish messages with data types other
 * than String
 *
 * @author Mark Fisher
 * @author Jennifer Hickey
 * @author Gary Russell
 */
public class RedisPublishingMessageHandlerTests {

	private static final String TOPIC = "si.test.channel";

	private static final int NUM_MESSAGES = 10;

	private RedisConnectionFactory connectionFactory;

	private RedisMessageListenerContainer container;

	private CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);

	@Rule
	public RedisTestSupport redisAvailableRule = new RedisTestSupport();

	@Before
	public void setUp() {
		this.connectionFactory = redisAvailableRule.getResource();
	}

	@Test
	public void testWithDefaultSerializer() throws Exception {
		setupListener(new StringRedisSerializer());
		final RedisPublishingMessageHandler handler = new RedisPublishingMessageHandler(connectionFactory);
		handler.setBeanFactory(BusTestUtils.MOCK_BF);
		handler.setTopic(TOPIC);
		handler.afterPropertiesSet();
		for (int i = 0; i < NUM_MESSAGES; i++) {
			handler.handleMessage(MessageBuilder.withPayload("test-" + i).build());
		}
		latch.await(3, TimeUnit.SECONDS);
		assertEquals(0, latch.getCount());
		container.stop();
	}

	@Test
	public void testWithNoSerializer() throws Exception {
		setupListener(null);
		final RedisPublishingMessageHandler handler = new RedisPublishingMessageHandler(connectionFactory);
		handler.setBeanFactory(BusTestUtils.MOCK_BF);
		handler.setTopic(TOPIC);
		handler.afterPropertiesSet();
		for (int i = 0; i < NUM_MESSAGES; i++) {
			handler.handleMessage(MessageBuilder.withPayload(new String("test-" + i).getBytes()).build());
		}
		latch.await(3, TimeUnit.SECONDS);
		assertEquals(0, latch.getCount());
		container.stop();
	}

	@Test
	public void testWithCustomSerializer() throws Exception {
		GenericToStringSerializer<Long> serializer = new GenericToStringSerializer<Long>(Long.class);
		setupListener(serializer);
		final RedisPublishingMessageHandler handler = new RedisPublishingMessageHandler(connectionFactory);
		handler.setBeanFactory(BusTestUtils.MOCK_BF);
		handler.setTopic(TOPIC);
		handler.setSerializer(serializer);
		handler.afterPropertiesSet();
		for (long i = 0; i < NUM_MESSAGES; i++) {
			handler.handleMessage(MessageBuilder.withPayload(i).build());
		}
		latch.await(3, TimeUnit.SECONDS);
		assertEquals(0, latch.getCount());
		container.stop();
	}

	private void setupListener(RedisSerializer<?> listenerSerializer) throws InterruptedException {
		MessageListenerAdapter listener = new MessageListenerAdapter();
		listener.setDelegate(new Listener(latch));
		listener.setSerializer(listenerSerializer);
		listener.afterPropertiesSet();

		this.container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.afterPropertiesSet();
		container.addMessageListener(listener, Collections.<Topic> singletonList(new ChannelTopic(TOPIC)));
		container.start();
		Thread.sleep(1000);
	}

	private static class Listener {

		private final CountDownLatch latch;

		private Listener(CountDownLatch latch) {
			this.latch = latch;
		}

		@SuppressWarnings("unused")
		public void handleMessage(Object s) {
			this.latch.countDown();
		}
	}

}
