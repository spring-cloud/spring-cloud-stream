/*
 * Copyright 2015 the original author or authors.
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
import java.util.Properties;
import java.util.Set;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
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
 */
public class RabbitTestBinder extends AbstractTestBinder<RabbitMessageChannelBinder> {

	public static final String BINDER_PREFIX = "binder.";

	private final RabbitAdmin rabbitAdmin;

	private final Set<String> prefixes = new HashSet<>();

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
	public void bindConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		capturePrefix(properties);
		super.bindConsumer(name, moduleInputChannel, properties);
	}

	@Override
	public void bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		capturePrefix(properties);
		super.bindProducer(name, moduleOutputChannel, properties);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inputChannel, String group, Properties properties) {
		capturePrefix(properties);
		super.bindPubSubConsumer(name, inputChannel, group, properties);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outputChannel, Properties properties) {
		capturePrefix(properties);
		super.bindPubSubProducer(name, outputChannel, properties);
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		capturePrefix(properties);
		super.bindRequestor(name, requests, replies, properties);
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		capturePrefix(properties);
		super.bindReplier(name, requests, replies, properties);
	}

	public void capturePrefix(Properties properties) {
		if (properties != null) {
			String prefix = properties.getProperty("prefix");
			if (prefix != null) {
				this.prefixes.add(prefix);
			}
		}
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
				for (String prefix : this.prefixes) {
					rabbitAdmin.deleteQueue(prefix + queue);
					// delete any partitioned queues
					for (int i = 0; i < 10; i++) {
						rabbitAdmin.deleteQueue(prefix + queue + "-" + i);
					}
					rabbitAdmin.deleteExchange(prefix + queue);
					rabbitAdmin.deleteExchange(prefix + queue + ".requests");
				}
				rabbitAdmin.deleteExchange(BINDER_PREFIX + queue);
			}
		}
		if (!topics.isEmpty()) {
			for (String exchange : topics) {
				rabbitAdmin.deleteExchange(BINDER_PREFIX + exchange);
			}
		}
	}
}
