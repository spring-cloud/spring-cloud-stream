/*
 * Copyright 2015-2016 the original author or authors.
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.test.junit.rabbit.RabbitTestSupport;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;

/**
 * @author Gary Russell
 * @since 1.2
 */
public class RabbitBinderCleanerTests {

	private static final String BINDER_PREFIX = "binder.";

	@Rule
	public RabbitTestSupport rabbitWithMgmtEnabled = new RabbitTestSupport(true);

	@Test
	public void testCleanStream() {
		final RabbitBindingCleaner cleaner = new RabbitBindingCleaner();
		final RestTemplate template = RabbitManagementUtils.buildRestTemplate("http://localhost:15672", "guest",
				"guest");
		final String stream1 = UUID.randomUUID().toString();
		String stream2 = stream1 + "-1";
		String firstQueue = null;
		CachingConnectionFactory connectionFactory = rabbitWithMgmtEnabled.getResource();
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		for (int i = 0; i < 5; i++) {
			String queue1Name = AbstractBinder.applyPrefix(BINDER_PREFIX, stream1 + ".default." + i);
			String queue2Name = AbstractBinder.applyPrefix(BINDER_PREFIX, stream2 + ".default." + i);
			if (firstQueue == null) {
				firstQueue = queue1Name;
			}
			URI uri = UriComponentsBuilder.fromUriString("http://localhost:15672/api/queues")
					.pathSegment("{vhost}", "{queue}")
					.buildAndExpand("/", queue1Name)
					.encode().toUri();
			template.put(uri, new AmqpQueue(false, true));
			uri = UriComponentsBuilder.fromUriString("http://localhost:15672/api/queues")
					.pathSegment("{vhost}", "{queue}")
					.buildAndExpand("/", queue2Name)
					.encode().toUri();
			template.put(uri, new AmqpQueue(false, true));
			uri = UriComponentsBuilder.fromUriString("http://localhost:15672/api/queues")
					.pathSegment("{vhost}", "{queue}")
					.buildAndExpand("/", AbstractBinder.constructDLQName(queue1Name)).encode().toUri();
			template.put(uri, new AmqpQueue(false, true));
			TopicExchange exchange = new TopicExchange(queue1Name);
			rabbitAdmin.declareExchange(exchange);
			rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(queue1Name)).to(exchange).with(queue1Name));
			exchange = new TopicExchange(queue2Name);
			rabbitAdmin.declareExchange(exchange);
			rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(queue2Name)).to(exchange).with(queue2Name));
		}
		final TopicExchange topic1 = new TopicExchange(
				AbstractBinder.applyPrefix(BINDER_PREFIX, stream1 + ".foo.bar"));
		rabbitAdmin.declareExchange(topic1);
		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(firstQueue)).to(topic1).with("#"));
		String foreignQueue = UUID.randomUUID().toString();
		rabbitAdmin.declareQueue(new Queue(foreignQueue));
		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(foreignQueue)).to(topic1).with("#"));
		final TopicExchange topic2 = new TopicExchange(
				AbstractBinder.applyPrefix(BINDER_PREFIX, stream2 + ".foo.bar"));
		rabbitAdmin.declareExchange(topic2);
		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(firstQueue)).to(topic2).with("#"));
		new RabbitTemplate(connectionFactory).execute(new ChannelCallback<Void>() {

			@Override
			public Void doInRabbit(Channel channel) throws Exception {
				String queueName = AbstractBinder.applyPrefix(BINDER_PREFIX, stream1 + ".default." + 4);
				String consumerTag = channel.basicConsume(queueName, new DefaultConsumer(channel));
				try {
					waitForConsumerStateNot(queueName, 0);
					cleaner.clean(stream1, false);
					fail("Expected exception");
				}
				catch (RabbitAdminException e) {
					assertEquals("Queue " + queueName + " is in use", e.getMessage());
				}
				channel.basicCancel(consumerTag);
				waitForConsumerStateNot(queueName, 1);
				try {
					cleaner.clean(stream1, false);
					fail("Expected exception");
				}
				catch (RabbitAdminException e) {
					assertThat(e.getMessage(), containsString("Cannot delete exchange "));
					assertThat(e.getMessage(), containsString("; it has bindings:"));
				}
				return null;
			}

			private void waitForConsumerStateNot(String queueName, int state) throws InterruptedException {
				int n = 0;
				URI uri = UriComponentsBuilder.fromUriString("http://localhost:15672/api/queues").pathSegment(
						"{vhost}", "{queue}")
						.buildAndExpand("/", queueName).encode().toUri();
				while (n++ < 100) {
					@SuppressWarnings("unchecked")
					Map<String, Object> queueInfo = template.getForObject(uri, Map.class);
					if (!queueInfo.get("consumers").equals(Integer.valueOf(state))) {
						break;
					}
					Thread.sleep(100);
				}
				assertTrue("Consumer state remained at " + state + " after 10 seconds", n < 100);
			}

		});
		rabbitAdmin.deleteExchange(topic1.getName()); // easier than deleting the binding
		rabbitAdmin.declareExchange(topic1);
		rabbitAdmin.deleteQueue(foreignQueue);
		connectionFactory.destroy();
		Map<String, List<String>> cleanedMap = cleaner.clean(stream1, false);
		assertEquals(2, cleanedMap.size());
		List<String> cleanedQueues = cleanedMap.get("queues");
		// should *not* clean stream2
		assertEquals(10, cleanedQueues.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(BINDER_PREFIX + stream1 + ".default." + i, cleanedQueues.get(i * 2));
			assertEquals(BINDER_PREFIX + stream1 + ".default." + i + ".dlq", cleanedQueues.get(i * 2 + 1));
		}
		List<String> cleanedExchanges = cleanedMap.get("exchanges");
		assertEquals(6, cleanedExchanges.size());

		// wild card *should* clean stream2
		cleanedMap = cleaner.clean(stream1 + "*", false);
		assertEquals(2, cleanedMap.size());
		cleanedQueues = cleanedMap.get("queues");
		assertEquals(5, cleanedQueues.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(BINDER_PREFIX + stream2 + ".default." + i, cleanedQueues.get(i));
		}
		cleanedExchanges = cleanedMap.get("exchanges");
		assertEquals(6, cleanedExchanges.size());
	}

	public static class AmqpQueue {

		private boolean autoDelete;

		private boolean durable;

		public AmqpQueue(boolean autoDelete, boolean durable) {
			this.autoDelete = autoDelete;
			this.durable = durable;
		}


		@JsonProperty("auto_delete")
		protected boolean isAutoDelete() {
			return autoDelete;
		}


		protected void setAutoDelete(boolean autoDelete) {
			this.autoDelete = autoDelete;
		}


		protected boolean isDurable() {
			return durable;
		}


		protected void setDurable(boolean durable) {
			this.durable = durable;
		}

	}

}
