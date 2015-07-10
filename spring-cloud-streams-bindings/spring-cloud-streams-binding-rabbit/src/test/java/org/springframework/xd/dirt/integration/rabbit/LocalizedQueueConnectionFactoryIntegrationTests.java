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

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.xd.test.rabbit.RabbitTestSupport;


/**
 *
 * @author Gary Russell
 */
public class LocalizedQueueConnectionFactoryIntegrationTests {

	@ClassRule
	public static RabbitTestSupport rabbitAvailableRule = new RabbitTestSupport(true);

	private LocalizedQueueConnectionFactory lqcf;

	@Before
	public void setup() {
		ConnectionFactory defaultConnectionFactory = new CachingConnectionFactory("localhost");
		String[] addresses = new String[] { "localhost:9999", "localhost:5672" };
		String[] adminAddresses = new String[] { "http://localhost:15672", "http://localhost:15672" };
		String[] nodes = new String[] { "foo@bar", "rabbit@localhost" };
		String vhost = "/";
		String username = "guest";
		String password = "guest";
		this.lqcf = new LocalizedQueueConnectionFactory(defaultConnectionFactory, addresses,
				adminAddresses, nodes, vhost, username, password, false, null);
	}

	@Test
	public void testConnect() {
		RabbitAdmin admin = new RabbitAdmin(this.lqcf);
		Queue queue = new Queue(UUID.randomUUID().toString(), false, false, true);
		admin.declareQueue(queue);
		ConnectionFactory targetConnectionFactory = this.lqcf.getTargetConnectionFactory("[" + queue.getName() + "]");
		RabbitTemplate template = new RabbitTemplate(targetConnectionFactory);
		template.convertAndSend("", queue.getName(), "foo");
		assertEquals("foo", template.receiveAndConvert(queue.getName()));
	}

}
