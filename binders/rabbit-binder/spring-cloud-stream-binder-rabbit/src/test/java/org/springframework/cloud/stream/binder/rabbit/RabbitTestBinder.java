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

import java.util.HashSet;
import java.util.Set;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.integration.autoconfigure.IntegrationAutoConfiguration;
import org.springframework.cloud.stream.binder.AbstractPollableConsumerTestBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.StringUtils;

/**
 * Test support class for {@link RabbitMessageChannelBinder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author David Turanski
 * @author Mark Fisher
 */
// @checkstyle:off
class RabbitTestBinder extends
		AbstractPollableConsumerTestBinder<RabbitMessageChannelBinder, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> {

	// @checkstyle:on
	private final RabbitAdmin rabbitAdmin;

	private final Set<String> prefixes = new HashSet<>();

	private final Set<String> queues = new HashSet<String>();

	private final Set<String> exchanges = new HashSet<String>();

	private final AnnotationConfigApplicationContext applicationContext;

	RabbitTestBinder(ConnectionFactory connectionFactory,
			RabbitProperties rabbitProperties) {
		this(connectionFactory, new RabbitMessageChannelBinder(connectionFactory,
				rabbitProperties, new RabbitExchangeQueueProvisioner(connectionFactory)));
	}

	RabbitTestBinder(ConnectionFactory connectionFactory,
			RabbitMessageChannelBinder binder) {
		this.applicationContext = new AnnotationConfigApplicationContext(IntegrationAutoConfiguration.class, Config.class);
		binder.setApplicationContext(this.applicationContext);
		this.setPollableConsumerBinder(binder);
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
	}

	public AnnotationConfigApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group,
			MessageChannel moduleInputChannel,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		captureConsumerResources(name, group, properties);
		return super.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name,
			String group, PollableSource<MessageHandler> inboundBindTarget,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		captureConsumerResources(name, group, properties);
		return super.bindPollableConsumer(name, group, inboundBindTarget, properties);
	}

	private void captureConsumerResources(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		String[] names = null;
		if (group != null) {
			if (properties.getExtension().isQueueNameGroupOnly()) {
				this.queues.add(properties.getExtension().getPrefix() + group);
			}
			else {
				if (properties.isMultiplex()) {
					names = StringUtils.commaDelimitedListToStringArray(name);
					for (String nayme : names) {
						this.queues.add(properties.getExtension().getPrefix()
								+ nayme.trim() + "." + group);
					}
				}
				else {
					this.queues.add(
							properties.getExtension().getPrefix() + name + "." + group);
				}
			}
		}
		if (names != null) {
			for (String nayme : names) {
				this.exchanges.add(properties.getExtension().getPrefix() + nayme.trim());
			}
		}
		else {
			this.exchanges.add(properties.getExtension().getPrefix() + name);
		}
		this.prefixes.add(properties.getExtension().getPrefix());
		deadLetters(properties.getExtension());
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name,
			MessageChannel moduleOutputChannel,
			ExtendedProducerProperties<RabbitProducerProperties> properties) {
		this.queues.add(properties.getExtension().getPrefix() + name + ".default");
		this.exchanges.add(properties.getExtension().getPrefix() + name);
		if (properties.getRequiredGroups() != null) {
			for (String group : properties.getRequiredGroups()) {
				if (properties.getExtension().isQueueNameGroupOnly()) {
					this.queues.add(properties.getExtension().getPrefix() + group);
				}
				else {
					this.queues.add(
							properties.getExtension().getPrefix() + name + "." + group);
				}
			}
		}
		this.prefixes.add(properties.getExtension().getPrefix());
		deadLetters(properties.getExtension());
		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	private void deadLetters(RabbitCommonProperties properties) {
		if (properties.getDeadLetterExchange() != null) {
			this.exchanges.add(properties.getDeadLetterExchange());
		}
		if (properties.getDeadLetterQueueName() != null) {
			this.queues.add(properties.getDeadLetterQueueName());
		}
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
		this.applicationContext.close();
	}

	@Configuration(proxyBeanMethods = false)
	@EnableIntegration
	static class Config {

	}

}
