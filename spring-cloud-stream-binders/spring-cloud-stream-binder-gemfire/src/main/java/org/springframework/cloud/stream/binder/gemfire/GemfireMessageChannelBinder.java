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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
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
public class GemfireMessageChannelBinder extends MessageChannelBinderSupport {
	private static final Logger logger = LoggerFactory.getLogger(GemfireMessageChannelBinder.class);

	/**
	 * Postfix for message regions.
	 */
	public static final String MESSAGES_POSTFIX = "-messages";

	/**
	 * GemFire peer-to-peer cache.
	 */
	private volatile Cache cache;

	/**
	 * A {@link com.gemstone.gemfire.cache.CacheListener} implementation
	 * that is invoked when messages are published to a region.
	 */
	private final MessageListener messageListener = new MessageListener();

	/**
	 * Map of message source names to {@link QueueReader}.
	 *
	 * @see #bindConsumer(String, MessageChannel, Properties)
	 */
	private final Map<String, QueueReader> queueReaders = new ConcurrentHashMap<>();

	@Override
	protected void onInit() {
		// todo: obtain properties from the config classes
		Properties properties = new Properties();
		properties.put("locators", "localhost[7777]");
		properties.put("log-level", "warning");
		// todo: put application name once we figure out how to get it;
		// getApplicationName is returning a null/empty string
//		properties.put("name", super.getApplicationContext().getApplicationName());
		this.cache = new CacheFactory(properties).create();
		super.onInit();
	}

	/**
	 * Create a {@link Region} instance used for consuming {@link Message} objects.
	 * This region registers {@link #messageListener} as a cache listener which
	 * triggers message consumption when a message is added to the region.
	 *
	 * @param name name of the message region
	 * @return region for consuming messages
	 */
	private Region<MessageKey, Message<?>> createConsumerMessageRegion(String name)  {
		RegionFactory<MessageKey, Message<?>> factory = this.cache.createRegionFactory(RegionShortcut.PARTITION);
		factory.addCacheListener(messageListener);
		return factory.create(name + MESSAGES_POSTFIX);
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
		return factory.create(name + MESSAGES_POSTFIX);
	}

	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.debug("bindConsumer({})", name);
		MessageListener messageListener = new MessageListener();
		QueueReader queueReader = new QueueReader(createConsumerMessageRegion(name),
				inboundBindTarget, messageListener);
		this.queueReaders.put(name, queueReader);
		Executors.newSingleThreadExecutor(new CustomizableThreadFactory(name + "-QueueReader"))
				.submit(queueReader);
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		this.queueReaders.remove(name).shutdown();
		super.unbindConsumer(name, channel);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.debug("bindPubSubConsumer");
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		logger.debug("bindProducer({})", name);
		Assert.isInstanceOf(SubscribableChannel.class, outboundBindTarget);

		((SubscribableChannel) outboundBindTarget).subscribe(new SendingHandler(createProducerMessageRegion(name)));
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		logger.debug("bindPubSubProducer");
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		logger.debug("bindRequestor");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		logger.debug("bindReplier");
	}


	/**
	 * {@link com.gemstone.gemfire.cache.CacheListener} implementation that
	 * {@link Object#notifyAll() notifies} itself when a new entry is added
	 * to the region it is registered for.
	 */
	private static class MessageListener extends CacheListenerAdapter<MessageKey, Message<?>> {

		public MessageListener() {
		}

		@Override
		public synchronized void afterCreate(EntryEvent<MessageKey, Message<?>> event) {
			this.notifyAll();
		}
	}


	/**
	 * Reads {@link Message} objects from a {@link Region} and
	 * publishes them to a {@link MessageChannel}.
	 */
	private static class QueueReader implements Runnable {

		private final Region<MessageKey, Message<?>> messageRegion;

		private final MessageChannel messageChannel;

		private final MessageListener messageListener;

		private volatile boolean running = true;

		public QueueReader(Region<MessageKey, Message<?>> messageRegion,
				MessageChannel messageChannel, MessageListener messageListener) {
			this.messageRegion = messageRegion;
			this.messageChannel = messageChannel;
			this.messageListener = messageListener;
		}

		public boolean isRunning() {
			return this.running;
		}

		public void shutdown() {
			this.running = false;
		}

		@Override
		public void run() {
			// only messages that are present in this JVM will be processed
			Region<MessageKey, Message<?>> localMessageRegion =
					PartitionRegionHelper.getLocalData(this.messageRegion);

			while (isRunning()) {
				try {
					List<MessageKey> keys = new ArrayList<>(localMessageRegion.keySet());
					logger.debug("Fetched {} messages", keys.size());

					if (!keys.isEmpty()) {
						Collections.sort(keys);
						List<MessageKey> errorKeys = null;
						Map<MessageKey, Message<?>> messages = localMessageRegion.getAll(keys);
						for (MessageKey key : keys) {
							Message<?> message = messages.get(key);
							logger.debug("QueueReader({})", message);
							try {
								this.messageChannel.send(message);
							}
							catch (Exception e) {
								logger.warn("Exception processing message", e);
								if (errorKeys == null) {
									errorKeys = new ArrayList<>();
								}
								errorKeys.add(key);
							}
						}

						// remove messages that were processed without error
						// todo: consider adding un-processed messages to a "dead letter" region
						if (errorKeys != null) {
							keys.removeAll(errorKeys);
						}
						localMessageRegion.removeAll(keys);
					}

					synchronized (this.messageListener) {
						this.messageListener.wait(500);
					}
				}
				catch (InterruptedException e) {
					logger.warn("Thread interrupted", e);
					Thread.currentThread().interrupt();
					shutdown();
				}
			}
		}
	}


	/**
	 * {@link MessageHandler} implementation that publishes messages
	 * to a {@link Region}.
	 */
	private class SendingHandler implements MessageHandler {

		private final Region<MessageKey, Message<?>> messageRegion;

		private final AtomicLong sequence = new AtomicLong();

		private final String memberId;

		public SendingHandler(Region<MessageKey, Message<?>> messageRegion) {
			this.messageRegion = messageRegion;
			// the member id is in the following format:
			//    '192.168.1.3(11484)<v6>:35110'
			this.memberId = GemfireMessageChannelBinder.this.cache
					.getDistributedSystem().getDistributedMember().getId();
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			logger.debug("publishing message {}", message);
			this.messageRegion.putAll(Collections.singletonMap(nextMessageKey(), message));
		}

		private MessageKey nextMessageKey() {
			return new MessageKey(sequence.getAndIncrement(), memberId);
		}
	}

}
