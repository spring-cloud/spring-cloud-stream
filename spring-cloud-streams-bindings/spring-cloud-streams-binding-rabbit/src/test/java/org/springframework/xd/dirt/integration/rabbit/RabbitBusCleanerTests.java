/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.rabbit;


import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.xd.dirt.integration.bus.BusUtils;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport;
import org.springframework.xd.dirt.integration.bus.RabbitAdminException;
import org.springframework.xd.dirt.integration.bus.RabbitManagementUtils;
import org.springframework.xd.dirt.integration.bus.rabbit.RabbitBusCleaner;
import org.springframework.xd.test.rabbit.RabbitAdminTestSupport;
import org.springframework.xd.test.rabbit.RabbitTestSupport;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Gary Russell
 * @since 1.2
 */
public class RabbitBusCleanerTests {

	private static final String XDBUS_PREFIX = "xdbus.";

	@Rule
	public RabbitAdminTestSupport adminTest = new RabbitAdminTestSupport();

	@Rule
	public RabbitTestSupport test = new RabbitTestSupport();

	@Test
	public void testCleanStream() {
		final RabbitBusCleaner cleaner = new RabbitBusCleaner();
		final RestTemplate template = RabbitManagementUtils.buildRestTemplate("http://localhost:15672", "guest", 
				"guest");
		final String stream1 = UUID.randomUUID().toString();
		String stream2 = stream1 + "-1";
		String firstQueue = null;
		for (int i = 0; i < 5; i++) {
			String queue1Name = MessageBusSupport.applyPrefix(XDBUS_PREFIX,
					BusUtils.constructPipeName(stream1, i));
			String queue2Name = MessageBusSupport.applyPrefix(XDBUS_PREFIX,
					BusUtils.constructPipeName(stream2, i));
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
					.buildAndExpand("/", MessageBusSupport.constructDLQName(queue1Name)).encode().toUri();
			template.put(uri, new AmqpQueue(false, true));
		}
		CachingConnectionFactory connectionFactory = test.getResource();
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		final FanoutExchange fanout1 = new FanoutExchange(
				MessageBusSupport.applyPrefix(XDBUS_PREFIX, MessageBusSupport.applyPubSub(
						BusUtils.constructTapPrefix(stream1) + ".foo.bar")));
		rabbitAdmin.declareExchange(fanout1);
		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(firstQueue)).to(fanout1));
		final FanoutExchange fanout2 = new FanoutExchange(
				MessageBusSupport.applyPrefix(XDBUS_PREFIX, MessageBusSupport.applyPubSub(
						BusUtils.constructTapPrefix(stream2) + ".foo.bar")));
		rabbitAdmin.declareExchange(fanout2);
		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(firstQueue)).to(fanout2));
		new RabbitTemplate(connectionFactory).execute(new ChannelCallback<Void>() {

			@Override
			public Void doInRabbit(Channel channel) throws Exception {
				String queueName = MessageBusSupport.applyPrefix(XDBUS_PREFIX,
						BusUtils.constructPipeName(stream1, 4));
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
					assertThat(e.getMessage(), startsWith("Cannot delete exchange " +
							fanout1.getName() + "; it has bindings:"));
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
		rabbitAdmin.deleteExchange(fanout1.getName()); // easier than deleting the binding
		rabbitAdmin.declareExchange(fanout1);
		connectionFactory.destroy();
		Map<String, List<String>> cleanedMap = cleaner.clean(stream1, false);
		assertEquals(2, cleanedMap.size());
		List<String> cleanedQueues = cleanedMap.get("queues");
		// should *not* clean stream2
		assertEquals(10, cleanedQueues.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(XDBUS_PREFIX + stream1 + "." + i, cleanedQueues.get(i * 2));
			assertEquals(XDBUS_PREFIX + stream1 + "." + i + ".dlq", cleanedQueues.get(i * 2 + 1));
		}
		List<String> cleanedExchanges = cleanedMap.get("exchanges");
		assertEquals(1, cleanedExchanges.size());
		assertEquals(fanout1.getName(), cleanedExchanges.get(0));

		// wild card *should* clean stream2
		cleanedMap = cleaner.clean(stream1 + "*", false);
		assertEquals(2, cleanedMap.size());
		cleanedQueues = cleanedMap.get("queues");
		assertEquals(5, cleanedQueues.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(XDBUS_PREFIX + stream2 + "." + i, cleanedQueues.get(i));
		}
		cleanedExchanges = cleanedMap.get("exchanges");
		assertEquals(1, cleanedExchanges.size());
		assertEquals(fanout2.getName(), cleanedExchanges.get(0));
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
