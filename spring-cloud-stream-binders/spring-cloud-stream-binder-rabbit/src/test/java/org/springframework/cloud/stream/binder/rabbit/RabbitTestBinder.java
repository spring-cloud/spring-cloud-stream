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

package org.springframework.cloud.stream.binder.rabbit;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.cloud.stream.binder.AbstractTestBinder;


/**
 * Test support class for {@link RabbitMessageChannelBinder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author David Turanski
 */
public class RabbitTestBinder extends AbstractTestBinder<RabbitMessageChannelBinder> {

	public static final String BINDER_PREFIX = "binder.rabbit.";

	private final RabbitAdmin rabbitAdmin;

	public RabbitTestBinder(ConnectionFactory connectionFactory) {
		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(connectionFactory);
		GenericApplicationContext context = new GenericApplicationContext();
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.afterPropertiesSet();
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, scheduler);
		context.refresh();
		binder.setApplicationContext(context);
		binder.setCodec(new PojoCodec());
		this.setBinder(binder);
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
	}

	@Override
	public void cleanup() {
		if (!queues.isEmpty()) {
			for (String queue : queues) {
				rabbitAdmin.deleteQueue(BINDER_PREFIX + queue);
				// delete any partitioned queues
				for (int i = 0; i < 10; i++) {
					rabbitAdmin.deleteQueue(BINDER_PREFIX + queue + "-" + i);
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
				rabbitAdmin.deleteExchange(BINDER_PREFIX + exchange);
			}
		}
	}
}
