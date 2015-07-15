/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.bus;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.MessageChannel;


/**
 * Abstract class that adds test support for {@link MessageBus}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public abstract class AbstractTestMessageBus<C extends MessageBusSupport> implements MessageBus {

	protected Set<String> queues = new HashSet<String>();

	protected Set<String> topics = new HashSet<String>();

	private C messageBus;

	public void setMessageBus(C messageBus) {
		messageBus.setIntegrationEvaluationContext(new StandardEvaluationContext());
		try {
			messageBus.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize message bus", e);
		}
		this.messageBus = messageBus;
	}

	@Override
	public void bindConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		messageBus.bindConsumer(name, moduleInputChannel, properties);
		queues.add(name);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inputChannel, Properties properties) {
		messageBus.bindPubSubConsumer(name, inputChannel, properties);
		addTopic(name);
	}

	@Override
	public void bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		messageBus.bindProducer(name, moduleOutputChannel, properties);
		queues.add(name);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outputChannel, Properties properties) {
		messageBus.bindPubSubProducer(name, outputChannel, properties);
		addTopic(name);
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		messageBus.bindRequestor(name, requests, replies, properties);
		queues.add(name + ".requests");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		messageBus.bindReplier(name, requests, replies, properties);
		queues.add(name + ".requests");
	}

	private void addTopic(String topicName) {
		topics.add("topic." + topicName);
	}

	public C getCoreMessageBus() {
		return messageBus;
	}

	public abstract void cleanup();

	@Override
	public void unbindConsumers(String name) {
		messageBus.unbindConsumers(name);
	}

	@Override
	public void unbindProducers(String name) {
		messageBus.unbindProducers(name);
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		messageBus.unbindConsumer(name, channel);
	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		messageBus.unbindProducer(name, channel);
	}

	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		this.queues.add(name);
		return this.messageBus.bindDynamicProducer(name, properties);
	}

	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		this.topics.add(name);
		return this.messageBus.bindDynamicPubSubProducer(name, properties);
	}

	@Override
	public boolean isCapable(Capability capability) {
		return this.messageBus.isCapable(capability);
	}

	public MessageBus getMessageBus() {
		return this.messageBus;
	}

}
