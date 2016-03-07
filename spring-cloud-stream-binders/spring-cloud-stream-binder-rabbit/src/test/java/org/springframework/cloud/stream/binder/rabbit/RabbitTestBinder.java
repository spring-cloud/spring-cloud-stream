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

import java.util.HashSet;
import java.util.Set;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Test support class for {@link RabbitMessageChannelBinder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author David Turanski
 * @author Mark Fisher
 */
public class RabbitTestBinder extends AbstractTestBinder<RabbitMessageChannelBinder, RabbitConsumerProperties, RabbitProducerProperties> {

	private final RabbitAdmin rabbitAdmin;

	private final Set<String> prefixes = new HashSet<>();

	private final Set<String> queues = new HashSet<String>();

	private final Set<String> exchanges = new HashSet<String>();

	public RabbitTestBinder(ConnectionFactory connectionFactory) {
		this(connectionFactory, new RabbitMessageChannelBinder(connectionFactory));
	}

	public RabbitTestBinder(ConnectionFactory connectionFactory, RabbitMessageChannelBinder binder) {
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
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel, RabbitConsumerProperties properties) {
		if (group != null) {
			this.queues.add(properties.getPrefix() + name + ("." + group));
		}
		this.exchanges.add(properties.getPrefix() + name);
		return super.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel, RabbitProducerProperties properties) {
		this.queues.add(properties.getPrefix() + name + ".default");
		this.exchanges.add(properties.getPrefix() + name);
		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	@Override
	public void cleanup() {
		for (String queue : this.queues) {
			this.rabbitAdmin.deleteQueue(queue);
			this.rabbitAdmin.deleteQueue(queue + ".dlq");
			// delete any partitioned queues
			for (int i = 0; i < 10; i++) {
				this.rabbitAdmin.deleteQueue(queue + "-" + i);
				this.rabbitAdmin.deleteQueue(queue + "-" + i + ".dlq");
			}
		}
		for (String exchange : this.exchanges) {
			this.rabbitAdmin.deleteExchange(exchange);
		}
		for (String prefix : this.prefixes) {
			this.rabbitAdmin.deleteExchange(prefix + "DLX");
		}
	}

}
