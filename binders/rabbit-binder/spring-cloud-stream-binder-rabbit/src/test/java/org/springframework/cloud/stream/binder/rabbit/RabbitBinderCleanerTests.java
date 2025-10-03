/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.rabbit.admin.RabbitAdminException;
import org.springframework.cloud.stream.binder.rabbit.admin.RabbitBindingCleaner;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Chris Bono
 * @since 1.2
 */
class RabbitBinderCleanerTests {

	private static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	private static final String BINDER_PREFIX = "binder.";

	private static final WebClient client;

	static {
		client = WebClient.builder()
				.filter(ExchangeFilterFunctions
						.basicAuthentication(RABBITMQ.getAdminUsername(), RABBITMQ.getAdminPassword()))
				.build();
	}

	@RegisterExtension
	private final RabbitTestSupport rabbitTestSupport = new RabbitTestSupport(true, RABBITMQ.getAmqpPort(),
			RABBITMQ.getHttpPort());

	@Test
	void cleanStream() {
		final RabbitBindingCleaner cleaner = new RabbitBindingCleaner();
		final String stream1 = new Base64UrlNamingStrategy("foo").generateName();
		String stream2 = stream1 + "-1";
		String firstQueue = null;
		CachingConnectionFactory connectionFactory = rabbitTestSupport.getResource();
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		for (int i = 0; i < 5; i++) {
			String queue1Name = AbstractBinder.applyPrefix(BINDER_PREFIX,
					stream1 + ".default." + i);
			String queue2Name = AbstractBinder.applyPrefix(BINDER_PREFIX,
					stream2 + ".default." + i);
			if (firstQueue == null) {
				firstQueue = queue1Name;
			}
			rabbitAdmin.declareQueue(new Queue(queue1Name, true, false, false));
			rabbitAdmin.declareQueue(new Queue(queue2Name, true, false, false));
			rabbitAdmin.declareQueue(new Queue(AbstractBinder.constructDLQName(queue1Name), true, false, false));
			TopicExchange exchange = new TopicExchange(queue1Name);
			rabbitAdmin.declareExchange(exchange);
			rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(queue1Name))
					.to(exchange).with(queue1Name));
			exchange = new TopicExchange(queue2Name);
			rabbitAdmin.declareExchange(exchange);
			rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(queue2Name))
					.to(exchange).with(queue2Name));
		}
		final TopicExchange topic1 = new TopicExchange(
				AbstractBinder.applyPrefix(BINDER_PREFIX, stream1 + ".foo.bar"));
		rabbitAdmin.declareExchange(topic1);
		rabbitAdmin.declareBinding(
				BindingBuilder.bind(new Queue(firstQueue)).to(topic1).with("#"));
		String foreignQueue = UUID.randomUUID().toString();
		rabbitAdmin.declareQueue(new Queue(foreignQueue));
		rabbitAdmin.declareBinding(
				BindingBuilder.bind(new Queue(foreignQueue)).to(topic1).with("#"));
		final TopicExchange topic2 = new TopicExchange(
				AbstractBinder.applyPrefix(BINDER_PREFIX, stream2 + ".foo.bar"));
		rabbitAdmin.declareExchange(topic2);
		rabbitAdmin.declareBinding(
				BindingBuilder.bind(new Queue(firstQueue)).to(topic2).with("#"));
		new RabbitTemplate(connectionFactory).execute(new ChannelCallback<Void>() {

			@Override
			public Void doInRabbit(Channel channel) throws Exception {
				String queueName = AbstractBinder.applyPrefix(BINDER_PREFIX,
						stream1 + ".default." + 4);
				String consumerTag = channel.basicConsume(queueName,
						new DefaultConsumer(channel));
				try {
					waitForConsumerState(queueName, 1);
					doClean(cleaner, stream1, false);
					Assertions.fail("Expected exception");
				}
				catch (RabbitAdminException e) {
					assertThat(e)
							.hasMessageContaining("Queue " + queueName + " is in use");
				}
				channel.basicCancel(consumerTag);
				waitForConsumerState(queueName, 0);
				try {
					doClean(cleaner, stream1, false);
					Assertions.fail("Expected exception");
				}
				catch (RabbitAdminException e) {
					assertThat(e).hasMessageContaining("Cannot delete exchange ");
					assertThat(e).hasMessageContaining("; it has bindings:");
				}
				return null;
			}

			private void waitForConsumerState(String queueName, long state)
					throws InterruptedException, URISyntaxException {

				int n = 0;
				Map<String, Object> queue = getQueue("/",  queueName);
				while (n++ < 100 && !requiredState(state, queue)) {
					Thread.sleep(100);
					queue = getQueue("/",  queueName);
				}
				assertThat(n).withFailMessage(
						"Consumer state remained at " + state + " after 10 seconds")
						.isLessThan(100);
			}

			private boolean requiredState(long state, Map<String, Object> queue) {
				Object consumers = queue.get("consumers");
				return state == 0
						? consumers == null || (Integer) consumers == 0
						: consumers != null && (Integer) consumers == state;
			}

		});
		rabbitAdmin.deleteExchange(topic1.getName()); // easier than deleting the binding
		rabbitAdmin.declareExchange(topic1);
		rabbitAdmin.deleteQueue(foreignQueue);
		connectionFactory.destroy();
		Map<String, List<String>> cleanedMap = doClean(cleaner, stream1, false);
		assertThat(cleanedMap).hasSize(2);
		List<String> cleanedQueues = cleanedMap.get("queues");
		// should *not* clean stream2
		assertThat(cleanedQueues).hasSize(10);
		for (int i = 0; i < 5; i++) {
			assertThat(cleanedQueues.get(i * 2))
					.isEqualTo(BINDER_PREFIX + stream1 + ".default." + i);
			assertThat(cleanedQueues.get(i * 2 + 1))
					.isEqualTo(BINDER_PREFIX + stream1 + ".default." + i + ".dlq");
		}
		List<String> cleanedExchanges = cleanedMap.get("exchanges");
		assertThat(cleanedExchanges).hasSize(6);

		// wild card *should* clean stream2
		cleanedMap = doClean(cleaner, stream1 + "*", false);
		assertThat(cleanedMap).hasSize(2);
		cleanedQueues = cleanedMap.get("queues");
		assertThat(cleanedQueues).hasSize(5);
		for (int i = 0; i < 5; i++) {
			assertThat(cleanedQueues.get(i))
					.isEqualTo(BINDER_PREFIX + stream2 + ".default." + i);
		}
		cleanedExchanges = cleanedMap.get("exchanges");
		assertThat(cleanedExchanges).hasSize(6);
	}

	protected Map<String, Object> getQueue(String string, String queueName) throws URISyntaxException {
		URI uri = new URI(RABBITMQ.getHttpUrl())
				.resolve("/api/queues/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8) + "/" + queueName);
		return client.get()
				.uri(uri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	private static Map<String, List<String>> doClean(RabbitBindingCleaner cleaner, String entity, boolean isJob) {
		return cleaner.clean(RABBITMQ.getHttpUrl() + "/api", RABBITMQ.getAdminUsername(), RABBITMQ.getAdminPassword(),
				"/", BINDER_PREFIX, entity, isJob);
	}

}
