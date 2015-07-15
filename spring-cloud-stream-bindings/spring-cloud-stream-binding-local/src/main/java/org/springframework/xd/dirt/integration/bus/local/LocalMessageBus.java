/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus.local;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.context.NamedComponent;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.xd.dirt.integration.bus.AbstractBusPropertiesAccessor;
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport;

/**
 * A simple implementation of {@link org.springframework.xd.dirt.integration.bus.MessageBus} for in-process use. For inbound and outbound, creates a
 * {@link DirectChannel} or a {@link QueueChannel} depending on whether the binding is aliased or not then bridges the
 * passed {@link MessageChannel} to the channel which is registered in the given application context. If that channel
 * does not yet exist, it will be created.
 *
 * @author David Turanski
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Ilayaperumal Gopinathan
 * @since 1.0
 */
public class LocalMessageBus extends MessageBusSupport {

	private static final int DEFAULT_EXECUTOR_CORE_POOL_SIZE = 0;

	private static final int DEFAULT_EXECUTOR_MAX_POOL_SIZE = 200;

	private static final int DEFAULT_EXECUTOR_QUEUE_SIZE = Integer.MAX_VALUE;

	private static final int DEFAULT_EXECUTOR_KEEPALIVE_SECONDS = 60;

	private static final int DEFAULT_REQ_REPLY_CONCURRENCY = 1;

	protected static final Set<Object> CONSUMER_REQUEST_REPLY_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.add(BusProperties.CONCURRENCY)
			.build();

	private volatile PollerMetadata poller;

	private final Map<String, ExecutorChannel> requestReplyChannels = new HashMap<String, ExecutorChannel>();

	private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

	private volatile int executorCorePoolSize = DEFAULT_EXECUTOR_CORE_POOL_SIZE;

	private volatile int executorMaxPoolSize = DEFAULT_EXECUTOR_MAX_POOL_SIZE;

	private volatile int executorQueueSize = DEFAULT_EXECUTOR_QUEUE_SIZE;

	private volatile int executorKeepAliveSeconds = DEFAULT_EXECUTOR_KEEPALIVE_SECONDS;

	private volatile int queueSize = Integer.MAX_VALUE;

	private final Map<String, ThreadPoolTaskExecutor> reqRepExecutors = new ConcurrentHashMap<>();

	/**
	 * Used to create and customize {@link QueueChannel}s when the binding operation involves aliased names.
	 */
	private final SharedChannelProvider<QueueChannel> queueChannelProvider = new SharedChannelProvider<QueueChannel>(
			QueueChannel.class) {

		@Override
		protected QueueChannel createSharedChannel(String name) {
			QueueChannel queueChannel = new QueueChannel(queueSize);
			return queueChannel;
		}
	};

	private final SharedChannelProvider<PublishSubscribeChannel> pubsubChannelProvider = new SharedChannelProvider<PublishSubscribeChannel>(
			PublishSubscribeChannel.class) {

		@Override
		protected PublishSubscribeChannel createSharedChannel(String name) {
			PublishSubscribeChannel publishSubscribeChannel = new PublishSubscribeChannel(executor);
			publishSubscribeChannel.setIgnoreFailures(true);
			return publishSubscribeChannel;
		}
	};

	/**
	 * Set the poller to use when QueueChannels are used.
	 */
	public void setPoller(PollerMetadata poller) {
		this.poller = poller;
	}

	/**
	 * Set the size of the queue when using {@link QueueChannel}s.
	 */
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	/**
	 * Set the {@link ThreadPoolTaskExecutor}} core pool size to limit the number of concurrent
	 * threads. The executor is used for PubSub operations.
	 * Default: 0 (threads created on demand until maxPoolSize).
	 * @param executorCorePoolSize the pool size.
	 */
	public void setExecutorCorePoolSize(int executorCorePoolSize) {
		this.executorCorePoolSize = executorCorePoolSize;
	}

	/**
	 * Set the {@link ThreadPoolTaskExecutor}} max pool size to limit the number of concurrent
	 * threads. The executor is used for PubSub operations.
	 * Default: 200.
	 * @param executorMaxPoolSize the pool size.
	 */
	public void setExecutorMaxPoolSize(int executorMaxPoolSize) {
		this.executorMaxPoolSize = executorMaxPoolSize;
	}

	/**
	 * Set the {@link ThreadPoolTaskExecutor}} queue size to limit the number of concurrent
	 * threads. The executor is used for PubSub operations.
	 * Default: {@link Integer#MAX_VALUE}.
	 * @param executorQueueSize the queue size.
	 */
	public void setExecutorQueueSize(int executorQueueSize) {
		this.executorQueueSize = executorQueueSize;
	}

	/**
	 * Set the {@link ThreadPoolTaskExecutor}} keep alive seconds.
	 * The executor is used for PubSub operations.
	 * @param executorKeepAliveSeconds the keep alive seconds.
	 */
	public void setExecutorKeepAliveSeconds(int executorKeepAliveSeconds) {
		this.executorKeepAliveSeconds = executorKeepAliveSeconds;
	}

	@Override
	protected void onInit() {
		this.executor.setCorePoolSize(this.executorCorePoolSize);
		this.executor.setMaxPoolSize(this.executorMaxPoolSize);
		this.executor.setQueueCapacity(this.executorQueueSize);
		this.executor.setKeepAliveSeconds(this.executorKeepAliveSeconds);
		this.executor.setThreadNamePrefix("xd.localbus-");
		this.executor.initialize();
	}

	/**
	 * For the local bus we bridge the router "output" channel to a queue channel; the queue
	 * channel gets the name and the source channel is named 'dynamic.output.to.' + name.
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		return doBindDynamicProducer(name, "dynamic.output.to." + name, properties);
	}

	/**
	 * For the local bus we bridge the router "output" channel to a pub/sub channel; the pub/sub
	 * channel gets the name and the source channel is named 'dynamic.output.to.' + name.
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		return doBindDynamicPubSubProducer(name, "dynamic.output.to." + name, properties);
	}

	private SharedChannelProvider<?> getChannelProvider(String name) {
		SharedChannelProvider<?> channelProvider = directChannelProvider;
		// Use queue channel provider in case of named channels:
		// point-to-point type syntax (queue:) and job input channel syntax (job:)
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX) || name.startsWith(JOB_CHANNEL_TYPE_PREFIX)) {
			channelProvider = queueChannelProvider;
		}
		return channelProvider;
	}

	/**
	 * Looks up or creates a DirectChannel with the given name and creates a bridge from that channel to the provided
	 * channel instance.
	 */
	@Override
	public void bindConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		validateConsumerProperties(name, properties, CONSUMER_STANDARD_PROPERTIES);
		doRegisterConsumer(name, moduleInputChannel, getChannelProvider(name), properties);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		validateConsumerProperties(name, properties, CONSUMER_STANDARD_PROPERTIES);
		doRegisterConsumer(name, moduleInputChannel, this.pubsubChannelProvider, properties);
	}

	private void doRegisterConsumer(String name, MessageChannel moduleInputChannel,
			SharedChannelProvider<?> channelProvider, Properties properties) {
		Assert.hasText(name, "a valid name is required to register an inbound channel");
		Assert.notNull(moduleInputChannel, "channel must not be null");
		MessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel(name);
		bridge(name, registeredChannel, moduleInputChannel,
				"inbound." + ((NamedComponent) registeredChannel).getComponentName(),
				new LocalBusPropertiesAccessor(properties));
	}

	/**
	 * Looks up or creates a DirectChannel with the given name and creates a bridge to that channel from the provided
	 * channel instance.
	 */
	@Override
	public void bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		validateConsumerProperties(name, properties, PRODUCER_STANDARD_PROPERTIES);
		doRegisterProducer(name, moduleOutputChannel, getChannelProvider(name), properties);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel moduleOutputChannel,
			Properties properties) {
		validateConsumerProperties(name, properties, PRODUCER_STANDARD_PROPERTIES);
		doRegisterProducer(name, moduleOutputChannel, this.pubsubChannelProvider, properties);
	}

	private void doRegisterProducer(String name, MessageChannel moduleOutputChannel,
			SharedChannelProvider<?> channelProvider, Properties properties) {
		Assert.hasText(name, "a valid name is required to register an outbound channel");
		Assert.notNull(moduleOutputChannel, "channel must not be null");
		MessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel(name);
		bridge(name, moduleOutputChannel, registeredChannel,
				"outbound." + ((NamedComponent) registeredChannel).getComponentName(),
				new LocalBusPropertiesAccessor(properties));
	}

	@Override
	public void bindRequestor(final String name, MessageChannel requests, final MessageChannel replies,
			Properties properties) {
		validateConsumerProperties(name, properties, CONSUMER_REQUEST_REPLY_PROPERTIES);
		final MessageChannel requestChannel = this.findOrCreateRequestReplyChannel(name, "requestor.", properties);
		// TODO: handle Pollable ?
		Assert.isInstanceOf(SubscribableChannel.class, requests);
		((SubscribableChannel) requests).subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				requestChannel.send(message);
			}
		});

		ExecutorChannel replyChannel = this.findOrCreateRequestReplyChannel(name, "replier.", properties);
		replyChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				replies.send(message);
			}
		});
	}

	@Override
	public void bindReplier(String name, final MessageChannel requests, MessageChannel replies,
			Properties properties) {
		validateConsumerProperties(name, properties, CONSUMER_REQUEST_REPLY_PROPERTIES);
		SubscribableChannel requestChannel = this.findOrCreateRequestReplyChannel(name, "requestor.", properties);
		requestChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				requests.send(message);
			}
		});

		// TODO: handle Pollable ?
		Assert.isInstanceOf(SubscribableChannel.class, replies);
		final SubscribableChannel replyChannel = this.findOrCreateRequestReplyChannel(name, "replier.", properties);
		((SubscribableChannel) replies).subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				replyChannel.send(message);
			}
		});
	}

	private synchronized ExecutorChannel findOrCreateRequestReplyChannel(String name, String prefix,
			Properties properties) {
		String channelName = prefix + name;
		ExecutorChannel channel = this.requestReplyChannels.get(channelName);
		if (channel == null) {
			ThreadPoolTaskExecutor executor = createRequestReplyExecutor(name, properties);
			channel = new ExecutorChannel(executor);
			channel.setBeanFactory(getBeanFactory());
			this.requestReplyChannels.put(channelName, channel);
			this.reqRepExecutors.put(name, executor);
		}
		return channel;
	}

	private ThreadPoolTaskExecutor createRequestReplyExecutor(String name, Properties properties) {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(new LocalBusPropertiesAccessor(properties).getConcurrency(DEFAULT_REQ_REPLY_CONCURRENCY));
		executor.setThreadNamePrefix("xd.localBus." + name + "-");
		executor.initialize();
		return executor;
	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		this.requestReplyChannels.remove("replier." + name);
		MessageChannel requestChannel = this.requestReplyChannels.remove("requestor." + name);
		if (requestChannel == null) {
			super.unbindProducer(name, channel);
		}
		ThreadPoolTaskExecutor executor = this.reqRepExecutors.remove(name);
		if (executor != null) {
			executor.shutdown();
		}
	}

	protected BridgeHandler bridge(String name, MessageChannel from, MessageChannel to, String bridgeName,
			LocalBusPropertiesAccessor properties) {
		return bridge(name, from, to, bridgeName, null, properties);
	}


	protected BridgeHandler bridge(String name, MessageChannel from, MessageChannel to, String bridgeName,
			final Collection<MimeType> acceptedMimeTypes, LocalBusPropertiesAccessor properties) {

		final boolean isInbound = bridgeName.startsWith("inbound.");

		BridgeHandler handler = new BridgeHandler() {

			@Override
			protected boolean shouldCopyRequestHeaders() {
				return false;
			}

			@Override
			protected Object handleRequestMessage(Message<?> requestMessage) {
				return requestMessage;
			}

		};

		handler.setBeanFactory(getBeanFactory());
		handler.setOutputChannel(to);
		handler.setBeanName(bridgeName);
		handler.afterPropertiesSet();

		// Usage of a CEFB allows to handle both Subscribable & Pollable channels the same way
		ConsumerEndpointFactoryBean cefb = new ConsumerEndpointFactoryBean();
		cefb.setInputChannel(from);
		cefb.setHandler(handler);
		cefb.setBeanFactory(getBeanFactory());
		if (from instanceof PollableChannel) {
			cefb.setPollerMetadata(poller);
		}
		try {
			cefb.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}

		try {
			cefb.getObject().setComponentName(handler.getComponentName());
			Binding binding = isInbound ? Binding.forConsumer(name, cefb.getObject(), to, properties)
					: Binding.forProducer(name, from, cefb.getObject(), properties);
			addBinding(binding);
			binding.start();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return handler;
	}

	protected <T> T getBean(String name, Class<T> requiredType) {
		return getApplicationContext().getBean(name, requiredType);
	}

	private static class LocalBusPropertiesAccessor extends AbstractBusPropertiesAccessor {

		public LocalBusPropertiesAccessor(Properties properties) {
			super(properties);
		}

	}

}
