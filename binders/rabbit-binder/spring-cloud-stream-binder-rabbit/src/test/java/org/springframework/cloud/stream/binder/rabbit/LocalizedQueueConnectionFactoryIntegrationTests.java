/*
 * Copyright 2015-2022 the original author or authors.
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

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Chris Bono
 */
public class LocalizedQueueConnectionFactoryIntegrationTests {

	private static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	@RegisterExtension
	private RabbitTestSupport rabbitTestSupport = new RabbitTestSupport(true, RABBITMQ.getAmqpPort(), RABBITMQ.getHttpPort());

	private LocalizedQueueConnectionFactory lqcf;

	@BeforeEach
	public void setup() {
		ConnectionFactory defaultConnectionFactory = rabbitTestSupport.getResource();
		String[] addresses = new String[] { "localhost:9999", "localhost:" + RABBITMQ.getAmqpPort() };
		String[] adminAddresses = new String[] { RABBITMQ.getHttpUrl(), RABBITMQ.getHttpUrl() };
		String[] nodes = new String[] { "foo@bar", "rabbit@localhost" };
		String vhost = "/";
		String username = "guest";
		String password = "guest";
		this.lqcf = new LocalizedQueueConnectionFactory(defaultConnectionFactory,
				addresses, adminAddresses, nodes, vhost, username, password, false, null,
				null, null, null);
	}

	@Test
	public void testConnect() {
		RabbitAdmin admin = new RabbitAdmin(this.lqcf);
		Queue queue = new Queue(UUID.randomUUID().toString(), false, false, true);
		admin.declareQueue(queue);
		ConnectionFactory targetConnectionFactory = this.lqcf
				.getTargetConnectionFactory("[" + queue.getName() + "]");
		RabbitTemplate template = new RabbitTemplate(targetConnectionFactory);
		template.convertAndSend("", queue.getName(), "foo");
		assertThat(template.receiveAndConvert(queue.getName())).isEqualTo("foo");
		((CachingConnectionFactory) targetConnectionFactory).destroy();
		admin.deleteQueue(queue.getName());
	}

}
