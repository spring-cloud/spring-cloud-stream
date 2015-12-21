/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.local;

import java.util.Collection;
import java.util.Properties;

import org.springframework.cloud.stream.binder.AbstractBindingPropertiesAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.context.NamedComponent;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

/**
 * A simple implementation of {@link org.springframework.cloud.stream.binder.Binder} for in-process use. For inbound and outbound, creates a
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
public class LocalMessageChannelBinder extends MessageChannelBinderSupport {

	public static final String THREAD_NAME_PREFIX = "binder.local-";

	private static final int DEFAULT_EXECUTOR_CORE_POOL_SIZE = 0;

	private static final int DEFAULT_EXECUTOR_MAX_POOL_SIZE = 200;

	private static final int DEFAULT_EXECUTOR_QUEUE_SIZE = Integer.MAX_VALUE;

	private static final int DEFAULT_EXECUTOR_KEEPALIVE_SECONDS = 60;

	private volatile PollerMetadata poller;

	private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

	private volatile int executorCorePoolSize = DEFAULT_EXECUTOR_CORE_POOL_SIZE;

	private volatile int executorMaxPoolSize = DEFAULT_EXECUTOR_MAX_POOL_SIZE;

	private volatile int executorQueueSize = DEFAULT_EXECUTOR_QUEUE_SIZE;

	private volatile int executorKeepAliveSeconds = DEFAULT_EXECUTOR_KEEPALIVE_SECONDS;

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
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		this.executor.setCorePoolSize(this.executorCorePoolSize);
		this.executor.setMaxPoolSize(this.executorMaxPoolSize);
		this.executor.setQueueCapacity(this.executorQueueSize);
		this.executor.setKeepAliveSeconds(this.executorKeepAliveSeconds);
		this.executor.setThreadNamePrefix(THREAD_NAME_PREFIX);
		this.executor.initialize();
	}

	@Override
	protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel moduleInputChannel,
			Properties properties) {
		validateConsumerProperties(name, properties, CONSUMER_STANDARD_PROPERTIES);
		return doRegisterConsumer(name, moduleInputChannel, this.pubsubChannelProvider, properties);
	}

	private Binding<MessageChannel> doRegisterConsumer(String name, MessageChannel moduleInputChannel,
			SharedChannelProvider<?> channelProvider, Properties properties) {
		Assert.hasText(name, "a valid name is required to register an inbound channel");
		Assert.notNull(moduleInputChannel, "channel must not be null");
		MessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel("localbinder." + name);
		bridge(name, registeredChannel, moduleInputChannel,
				"inbound." + ((NamedComponent) registeredChannel).getComponentName(),
				new LocalBindingPropertiesAccessor(properties));
		// TODO: ?
		return null;
	}

	/**
	 * Looks up or creates a DirectChannel with the given name and creates a bridge to that channel from the provided
	 * channel instance.
	 */
	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		validateConsumerProperties(name, properties, PRODUCER_STANDARD_PROPERTIES);
		return doRegisterProducer(name, moduleOutputChannel, this.pubsubChannelProvider, properties);
	}

	private Binding<MessageChannel> doRegisterProducer(String name, MessageChannel moduleOutputChannel,
			SharedChannelProvider<?> channelProvider, Properties properties) {
		Assert.hasText(name, "a valid name is required to register an outbound channel");
		Assert.notNull(moduleOutputChannel, "channel must not be null");
		MessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel("localbinder." + name);
		bridge(name, moduleOutputChannel, registeredChannel,
				"outbound." + ((NamedComponent) registeredChannel).getComponentName(),
				new LocalBindingPropertiesAccessor(properties));
		// TODO: ?
		return null;
	}

	@Override
	public void unbind(Binding<MessageChannel> binding) {
	}

	protected BridgeHandler bridge(String name, MessageChannel from, MessageChannel to, String bridgeName,
			LocalBindingPropertiesAccessor properties) {
		return bridge(name, from, to, bridgeName, null, properties);
	}


	protected BridgeHandler bridge(String name, MessageChannel from, MessageChannel to, String bridgeName,
			final Collection<MimeType> acceptedMimeTypes, LocalBindingPropertiesAccessor properties) {

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
			Binding<MessageChannel> binding = isInbound ? Binding.forConsumer(name, null, cefb.getObject(), to, properties)
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

	private static class LocalBindingPropertiesAccessor extends AbstractBindingPropertiesAccessor {

		public LocalBindingPropertiesAccessor(Properties properties) {
			super(properties);
		}

	}

}
