/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.messaging.MessageChannel;


/**
 * Abstract class that adds test support for {@link Binder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public abstract class AbstractTestBinder<C extends MessageChannelBinderSupport> implements Binder<MessageChannel> {

	protected Set<String> queues = new HashSet<String>();

	protected Set<String> topics = new HashSet<String>();

	private C binder;

	public void setBinder(C binder) {
		try {
			binder.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize binder", e);
		}
		this.binder = binder;
	}

	@Override
	public void bindConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		binder.bindConsumer(name, moduleInputChannel, properties);
		queues.add(name);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inputChannel, Properties properties) {
		binder.bindPubSubConsumer(name, inputChannel, properties);
		addTopic(name);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inputChannel, String group, Properties properties) {
		binder.bindPubSubConsumer(name, inputChannel, group, properties);
		addTopic(name);
	}

	@Override
	public void bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		binder.bindProducer(name, moduleOutputChannel, properties);
		queues.add(name);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outputChannel, Properties properties) {
		binder.bindPubSubProducer(name, outputChannel, properties);
		addTopic(name);
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		binder.bindRequestor(name, requests, replies, properties);
		queues.add(name + ".requests");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		binder.bindReplier(name, requests, replies, properties);
		queues.add(name + ".requests");
	}

	private void addTopic(String topicName) {
		topics.add("topic." + topicName);
	}

	public C getCoreBinder() {
		return binder;
	}

	public abstract void cleanup();

	@Override
	public void unbindConsumers(String name) {
		binder.unbindConsumers(name);
	}

	@Override
	public void unbindPubSubConsumers(String name, String group) {
		binder.unbindPubSubConsumers(name, group);
	}

	@Override
	public void unbindProducers(String name) {
		binder.unbindProducers(name);
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		binder.unbindConsumer(name, channel);
	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		binder.unbindProducer(name, channel);
	}

	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		this.queues.add(name);
		return this.binder.bindDynamicProducer(name, properties);
	}

	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		this.topics.add(name);
		return this.binder.bindDynamicPubSubProducer(name, properties);
	}

	public C getBinder() {
		return this.binder;
	}

}
