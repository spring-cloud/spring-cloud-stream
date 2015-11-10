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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;


/**
 * A binder that uses <a href="http://gemfire.docs.pivotal.io/">GemFire</a>
 * for message delivery. Spring Cloud Stream modules that are of type
 * processor or sink will host buckets for a partitioned region used
 * to store {@link Message messages}. This allows for message delivery
 * directly to the modules that will process them (as opposed to fetching
 * messages from a stand-alone messaging system).
 *
 * @author Patrick Peralta
 */
public class GemfireMessageChannelBinder implements Binder<MessageChannel>, ApplicationContextAware, InitializingBean {
	private static final Logger logger = LoggerFactory.getLogger(GemfireMessageChannelBinder.class);

	/**
	 * Postfix for message regions.
	 */
	public static final String MESSAGES_POSTFIX = "-messages";

	/**
	 * Postfix for region event queues.
	 */
	public static final String QUEUE_POSTFIX = "-queue";

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
	 * Map of message regions.
	 */
	private Map<String, Region<MessageKey, Message<?>>> regionMap = new ConcurrentHashMap<>();


	@Override
	public void afterPropertiesSet() throws Exception {
		// todo: obtain properties from the config classes
		Properties properties = new Properties();
		properties.put("locators", "localhost[7777]");
		properties.put("log-level", "warning");
		properties.put("mcast-port", "0");
		properties.put("name", this.applicationContext.getId());
		this.cache = new CacheFactory(properties).create();
	}

	/**
	 * Create a {@link Region} instance used for consuming {@link Message} objects.
	 * This region registers a {@link AsyncMessageListener} as a cache listener which
	 * triggers message consumption when a message is added to the region.
	 *
	 * @param name prefix of the message region name
	 * @return region for consuming messages
	 */
	private Region<MessageKey, Message<?>> createConsumerMessageRegion(String name)  {
		RegionFactory<MessageKey, Message<?>> regionFactory = this.cache.createRegionFactory(RegionShortcut.PARTITION);
		return regionFactory.addAsyncEventQueueId(name + QUEUE_POSTFIX).create(name + MESSAGES_POSTFIX);
	}

	/**
	 * Create a {@link AsyncEventQueue} for passing messages to the provided
	 * {@link AsyncMessageListener}.
	 *
	 * @param name prefix of the event queue name
	 * @param messageListener message listener invoked when an event is added to the queue
	 * @return queue for processing region events
	 */
	private AsyncEventQueue createAsyncEventQueue(String name, AsyncMessageListener messageListener) {
		AsyncEventQueueFactory queueFactory = this.cache.createAsyncEventQueueFactory();
		queueFactory.setPersistent(false);
		queueFactory.setParallel(false);
		queueFactory.setBatchSize(this.batchSize);
		String queueId = name + QUEUE_POSTFIX;
		return queueFactory.create(queueId, messageListener);
	}

	/**
	 * Create a {@link Region} instance used for publishing {@link Message} objects.
	 * This region instance will not store buckets; it is assumed that the regions
	 * created by consumers will host buckets.
	 *
	 * @param name name of the message region
	 * @return region for producing messages
	 */
	private Region<MessageKey, Message<?>> createProducerMessageRegion(String name) {
		RegionFactory<MessageKey, Message<?>> factory = this.cache.createRegionFactory(RegionShortcut.PARTITION_PROXY);
		return factory.addAsyncEventQueueId(name + QUEUE_POSTFIX).create(name + MESSAGES_POSTFIX);
	}

	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.debug("bindConsumer({})", name);
		AsyncMessageListener messageListener = new AsyncMessageListener(inboundBindTarget);
		createAsyncEventQueue(name, messageListener);
		this.regionMap.put(name, createConsumerMessageRegion(name));
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		Region<MessageKey, Message<?>> region = this.regionMap.get(name);
		if (region != null) {
			region.close();
		}
	}

	@Override
	public void unbindProducer(String name, MessageChannel outboundBindTarget) {
		Region<MessageKey, Message<?>> region = this.regionMap.get(name);
		if (region != null) {
			region.close();
		}
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		Assert.isInstanceOf(SubscribableChannel.class, outboundBindTarget);

		Region<MessageKey, Message<?>> producerRegion = createProducerMessageRegion(name);
		((SubscribableChannel) outboundBindTarget).subscribe(
				new SendingHandler(producerRegion,
						this.cache.getDistributedSystem().getDistributedMember().getProcessId()));
		this.regionMap.put(name, producerRegion);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
	}

	@Override
	public void unbindConsumers(String name) {
	}

	@Override
	public void unbindProducers(String name) {
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
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
	public boolean isCapable(Capability capability) {
		return false;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}


	/**
	 * Implementation of {@link AsyncEventListener} that passes {@link Message}s
	 * to a {@link MessageChannel}.
	 */
	private static class AsyncMessageListener implements AsyncEventListener {

		private final MessageChannel messageChannel;

		public AsyncMessageListener(MessageChannel messageChannel) {
			this.messageChannel = messageChannel;
		}

		@Override
		public boolean processEvents(List<AsyncEvent> events) {
			logger.trace("Received {} events", events.size());
			logger.trace("Events: {}", events);
			for (AsyncEvent event : events) {
				if (event.getOperation() == Operation.CREATE
						|| event.getOperation() == Operation.PUTALL_CREATE) {
					MessageKey key = (MessageKey) event.getKey();
					Message<?> message = (Message<?>) event.getDeserializedValue();
					try {
						this.messageChannel.send(message);
						event.getRegion().remove(key);
					}
					catch (Exception e) {
						logger.error("Exception processing message", e);
						// todo: add retry logic
						// todo: add un-delivered messages to a "dead letter" region
					}
				}
			}
			return true;
		}

		@Override
		public void close() {
		}
	}


	/**
	 * {@link MessageHandler} implementation that publishes messages
	 * to a {@link Region}.
	 */
	private static class SendingHandler implements MessageHandler {

		private final Region<MessageKey, Message<?>> messageRegion;

		private final AtomicLong sequence = new AtomicLong();

		private final int pid;

		private final long timestamp = System.currentTimeMillis();

		public SendingHandler(Region<MessageKey, Message<?>> messageRegion, int pid) {
			this.messageRegion = messageRegion;
			this.pid = pid;
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			logger.trace("Publishing message {}", message);
			this.messageRegion.putAll(Collections.singletonMap(nextMessageKey(), message));
		}

		private MessageKey nextMessageKey() {
			return new MessageKey(sequence.getAndIncrement(), timestamp, pid);
		}
	}

}
