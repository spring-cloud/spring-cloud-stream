/*
 * Copyright 2014 the original author or authors.
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

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.xd.dirt.integration.bus.AbstractTestMessageBus;
import org.springframework.xd.dirt.integration.bus.rabbit.RabbitMessageBus;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;


/**
 * Test support class for {@link org.springframework.xd.dirt.integration.bus.rabbit.RabbitMessageBus}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class RabbitTestMessageBus extends AbstractTestMessageBus<RabbitMessageBus> {

	private final RabbitAdmin rabbitAdmin;

	public RabbitTestMessageBus(ConnectionFactory connectionFactory) {
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
	}

	public RabbitTestMessageBus(ConnectionFactory connectionFactory, MultiTypeCodec<Object> codec) {
		RabbitMessageBus messageBus = new RabbitMessageBus(connectionFactory, codec);
		GenericApplicationContext context = new GenericApplicationContext();
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.afterPropertiesSet();
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, scheduler);
		context.refresh();
		messageBus.setApplicationContext(context);
		this.setMessageBus(messageBus);
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
	}

	@Override
	public void cleanup() {
		if (!queues.isEmpty()) {
			for (String queue : queues) {
				rabbitAdmin.deleteQueue("xdbus." + queue);
				// delete any partitioned queues
				for (int i = 0; i < 10; i++) {
					rabbitAdmin.deleteQueue("xdbus." + queue + "-" + i);
				}
				rabbitAdmin.deleteQueue("foo." + queue);
				// delete any partitioned queues
				for (int i = 0; i < 10; i++) {
					rabbitAdmin.deleteQueue("foo." + queue + "-" + i);
				}
			}
		}
		if (!topics.isEmpty()) {
			for (String exchange : topics) {
				rabbitAdmin.deleteExchange("xdbus." + exchange);
			}
		}
	}
}
