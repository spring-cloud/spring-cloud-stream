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

package org.springframework.cloud.stream.binder.gemfire;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * A binder that uses <a href="http://gemfire.docs.pivotal.io/">GemFire</a>
 * for message delivery. Spring Cloud Stream modules that are of type
 * processor or sink will host buckets for a partitioned region used
 * to store {@link Message messages}. This allows for message delivery
 * directly to the modules that will process them (as opposed to fetching
 * messages from a stand-alone messaging system).
 * <p>
 * Messages for each consumer group will be stored in their own partitioned
 * region. Consumer group metadata is stored in a replicated region.
 *
 * @author Patrick Peralta
 */
public class GemfireMessageChannelBinder implements Binder<MessageChannel>, ApplicationContextAware, InitializingBean {
	private static final Logger logger = LoggerFactory.getLogger(GemfireMessageChannelBinder.class);

	/**
	 * SPeL parser.
	 */
	private static final SpelExpressionParser parser = new SpelExpressionParser();

	/**
	 * Postfix for message regions.
	 */
	public static final String MESSAGES_POSTFIX = "-messages";

	/**
	 * Postfix for region event queues.
	 */
	public static final String QUEUE_POSTFIX = "-queue";

	/**
	 * Name of replicated region used to register consumer groups.
	 */
	public static final String CONSUMER_GROUPS_REGION = "consumer-groups-region";

	/**
	 * Application context that created this object.
	 */
	private volatile ApplicationContext applicationContext;

	/**
	 * GemFire peer-to-peer cache.
	 */
	private volatile Cache cache;

	// todo: make the following fields configurable

	/**
	 * Maximum number of messages to be fetched from the region
	 * for processing at a time.
	 */
	private volatile int batchSize = 1000;

	/**
	 * Map of message regions used for consuming messages.
	 */
	private final Map<String, Region<MessageKey, Message<?>>> regionMap = new ConcurrentHashMap<>();

	/**
	 * Map of registered {@link SendingHandler}s for producers.
	 */
	private final Map<String, SendingHandler> sendingHandlerMap = new ConcurrentHashMap<>();

	/**
	 * Replicated region for consumer group registration.
	 * Key is the binding name, value is the group name.
	 */
	private volatile Region<String, Set<String>> consumerGroupsRegion;


	@Override
	public void afterPropertiesSet() throws Exception {
		// todo: obtain properties from the config classes
		Properties properties = new Properties();
		properties.put("locators", "localhost[7777]");
		properties.put("log-level", "warning");
		properties.put("mcast-port", "0");
		this.cache = new CacheFactory(properties).create();
		RegionFactory<String, Set<String>> regionFactory = this.cache.createRegionFactory(RegionShortcut.REPLICATE);
		this.consumerGroupsRegion = regionFactory.create(CONSUMER_GROUPS_REGION);
	}

	/**
	 * For a binding name and consumer group name, return a string
	 * used for naming the region that will hold messages for this binding
	 * and consumer group.
	 *
	 * @param name binding name
	 * @param group consumer group name
	 * @return name of region for messages for this binding and consumer group
	 */
	public static String formatMessageRegionName(String name, String group) {
		return String.format("%s-%s", name, group);
	}

	/**
	 * Create a {@link Region} instance used for consuming {@link Message} objects.
	 *
	 * @param name prefix of the message region name
	 * @return region for consuming messages
	 */
	private Region<MessageKey, Message<?>> createConsumerMessageRegion(String name)  {
		RegionFactory<MessageKey, Message<?>> regionFactory = this.cache.createRegionFactory(RegionShortcut.PARTITION);
		return regionFactory.create(name + MESSAGES_POSTFIX);
	}

	/**
	 * Register a consumer group for a binding.
	 *
	 * @param name  binding name
	 * @param group consumer group name
	 */
	private void addConsumerGroup(String name, String group) {
		boolean groupAdded = false;
		while (!groupAdded) {
			Set<String> oldGroupSet = this.consumerGroupsRegion.get(name);
			Set<String> newGroupSet = new HashSet<>();
			newGroupSet.add(group);
			if (oldGroupSet == null) {
				groupAdded = this.consumerGroupsRegion.putIfAbsent(name, newGroupSet) == null;
			}
			else {
				newGroupSet.addAll(oldGroupSet);
				groupAdded = this.consumerGroupsRegion.replace(name, oldGroupSet, newGroupSet);
			}
		}
	}

	/**
	 * Remove registration for a consumer group for a binding.
	 *
	 * @param name  binding name
	 * @param group consumer group name
	 */
	private void removeConsumerGroup(String name, String group) {
		boolean groupRemoved = false;
		while (!groupRemoved) {
			Set<String> oldGroupSet = this.consumerGroupsRegion.get(name);
			if (oldGroupSet == null) {
				return;
			}
			Set<String> newGroupSet = new HashSet<>(oldGroupSet);
			newGroupSet.remove(group);
			groupRemoved = this.consumerGroupsRegion.replace(name, oldGroupSet, newGroupSet);
		}
	}

	/**
	 * Remove registration for all consumer groups for a binding.
	 *
	 * @param name binding name.
	 */
	private void removeConsumerGroups(String name) {
		this.consumerGroupsRegion.remove(name);
	}

	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		bindPubSubConsumer(name, inboundBindTarget, null, properties);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, String group, Properties properties) {
		if (StringUtils.isEmpty(group)) {
			group = "default";
		}
		String messageRegionName = formatMessageRegionName(name, group);

		Region<MessageKey, Message<?>> messageRegion = createConsumerMessageRegion(messageRegionName);
		/* todo: make configurable */
		AsyncEventQueueFactory queueFactory = this.cache.createAsyncEventQueueFactory();
		queueFactory.setPersistent(false);
		queueFactory.setParallel(false);
		queueFactory.setBatchSize(this.batchSize);

		AsyncEventListeningMessageProducer messageProducer =
				new AsyncEventListeningMessageProducer(messageRegion, queueFactory);
		messageProducer.setOutputChannel(inboundBindTarget);
		messageProducer.setExpressionPayload(parser.parseExpression("deserializedValue"));
		messageProducer.setBeanFactory(this.applicationContext);
		messageProducer.afterPropertiesSet();

		this.regionMap.put(name, messageRegion);
		addConsumerGroup(name, group);
		messageProducer.start();
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		bindPubSubProducer(name, outboundBindTarget, properties);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		Assert.isInstanceOf(SubscribableChannel.class, outboundBindTarget);

		SendingHandler handler = new SendingHandler(this.cache, this.consumerGroupsRegion, name);
		handler.start();

		((SubscribableChannel) outboundBindTarget).subscribe(handler);
		this.sendingHandlerMap.put(name, handler);
	}

	@Override
	public void unbindConsumers(String name) {
		for (String regionName : this.regionMap.keySet()) {
			if (regionName.startsWith(name)) {
				this.regionMap.remove(regionName).close();
			}
		}
		removeConsumerGroups(name);
	}

	@Override
	public void unbindPubSubConsumers(String name, String group) {
		this.regionMap.get(formatMessageRegionName(name, group)).close();
		removeConsumerGroup(name, group);
	}

	@Override
	public void unbindProducers(String name) {
		this.sendingHandlerMap.get(name).stop();
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		unbindConsumers(name);
	}

	@Override
	public void unbindProducer(String name, MessageChannel outboundBindTarget) {
		unbindProducers(name);
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

}
