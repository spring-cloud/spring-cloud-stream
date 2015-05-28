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

package org.springframework.bus.runner.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.bus.runner.config.MessageBusProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.ChannelInterceptorAware;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.XdHeaders;

/**
 * @author Mark Fisher
 * @author Dave Syer
 */
@ManagedResource
public class MessageBusAdapter implements Lifecycle, ApplicationContextAware {

	private static Logger logger = LoggerFactory.getLogger(MessageBusAdapter.class);

	private MessageBus messageBus;
	private MessageBuilderFactory messageBuilderFactory = new DefaultMessageBuilderFactory();
	private MessageChannel outputChannel;
	private MessageChannel inputChannel;

	private boolean running = false;

	private final AtomicBoolean active = new AtomicBoolean(false);

	private boolean trackHistory = false;

	private MessageBusProperties module;

	private ConfigurableApplicationContext applicationContext;

	public MessageBusAdapter(MessageBusProperties module, MessageBus messageBus) {
		this.module = module;
		this.messageBus = messageBus;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;

	}

	public void setMessageBuilderFactory(MessageBuilderFactory messageBuilderFactory) {
		this.messageBuilderFactory = messageBuilderFactory;
	}

	public void setTrackHistory(boolean trackHistory) {
		this.trackHistory = trackHistory;
	}

	public void setOutputChannel(MessageChannel outputChannel) {
		this.outputChannel = outputChannel;
	}

	public void setInputChannel(MessageChannel inputChannel) {
		this.inputChannel = inputChannel;
	}

	@Override
	@ManagedOperation
	public void start() {
		if (!running) {
			// Start everything, but don't call ourselves
			if (!active.get()) {
				if (active.compareAndSet(false, true)) {
					bindChannels();
					applicationContext.start();
					active.set(false);
				}
			}
		}
		running = true;
	}

	@Override
	@ManagedOperation
	public void stop() {
		if (running) {
			if (!active.get()) {
				if (active.compareAndSet(false, true)) {
					unbindChannels();
					applicationContext.stop();
					active.set(false);
				}
			}
		}
		running = false;
	}

	@Override
	@ManagedAttribute
	public boolean isRunning() {
		return running && applicationContext.isRunning();
	}

	protected final void unbindChannels() {
		if (inputChannel != null) {
			messageBus.unbindConsumers(module.getInputChannelName());
		}
		if (outputChannel != null) {
			messageBus.unbindProducers(module.getOutputChannelName());
			String tapChannelName = module.getTapChannelName();
			messageBus.unbindProducers(tapChannelName);
		}
	}

	protected final void bindChannels() {
		Map<String, Object> historyProperties = null;
		if (trackHistory) {
			// TODO: addHistoryTag();
		}
		if (outputChannel != null) {
			bindMessageProducer(outputChannel, module.getOutputChannelName(),
					module.getProducerProperties());
			String tapChannelName = module.getTapChannelName();
			// tappableChannels.put(tapChannelName, outputChannel);
			// if (isTapActive(tapChannelName)) {
			createAndBindTapChannel(tapChannelName, outputChannel);
			// }
			if (trackHistory) {
				track(outputChannel, historyProperties);
			}
		}
		if (inputChannel != null) {
			bindMessageConsumer(inputChannel, module.getInputChannelName(),
					module.getConsumerProperties());
			if (trackHistory && outputChannel==null) {
				track(inputChannel, historyProperties);
			}
		}
	}

	/*
	 * Following methods copied from parent to support the bindChannels() method above
	 */

	private void bindMessageConsumer(MessageChannel inputChannel,
			String inputChannelName, Properties consumerProperties) {
		if (isChannelPubSub(inputChannelName)) {
			messageBus.bindPubSubConsumer(inputChannelName, inputChannel,
					consumerProperties);
		}
		else {
			messageBus.bindConsumer(inputChannelName, inputChannel, consumerProperties);
		}
	}

	private void bindMessageProducer(MessageChannel outputChannel,
			String outputChannelName, Properties producerProperties) {
		if (isChannelPubSub(outputChannelName)) {
			messageBus.bindPubSubProducer(outputChannelName, outputChannel,
					producerProperties);
		}
		else {
			messageBus.bindProducer(outputChannelName, outputChannel, producerProperties);
		}
	}

	private boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName),
				"Channel name should not be empty/null.");
		return (channelName.startsWith("tap:") || channelName.startsWith("topic:"));
	}

	/**
	 * Creates a wiretap on the output channel and binds the tap channel to
	 * {@link MessageBus}'s message target.
	 *
	 * @param tapChannelName the name of the tap channel
	 * @param outputChannel the channel to tap
	 */
	private void createAndBindTapChannel(String tapChannelName,
			MessageChannel outputChannel) {
		logger.info("creating and binding tap channel for {}", tapChannelName);
		if (outputChannel instanceof ChannelInterceptorAware) {
			DirectChannel tapChannel = new DirectChannel();
			tapChannel.setBeanName(tapChannelName + ".tap.bridge");
			messageBus.bindPubSubProducer(tapChannelName, tapChannel, null); // TODO tap
																				// producer
																				// props
			tapOutputChannel(tapChannel, (ChannelInterceptorAware) outputChannel);
		}
		else {
			if (logger.isDebugEnabled()) {
				logger.debug("output channel is not interceptor aware. Tap will not be created.");
			}
		}
	}

	private MessageChannel tapOutputChannel(MessageChannel tapChannel,
			ChannelInterceptorAware outputChannel) {
		outputChannel.addInterceptor(new WireTap(tapChannel));
		return tapChannel;
	}

	private void track(MessageChannel channel, final Map<String, Object> historyProps) {
		if (channel instanceof ChannelInterceptorAware) {
			((ChannelInterceptorAware) channel)
					.addInterceptor(new ChannelInterceptorAdapter() {

						@Override
						public Message<?> preSend(Message<?> message,
								MessageChannel channel) {
							@SuppressWarnings("unchecked")
							Collection<Map<String, Object>> history = (Collection<Map<String, Object>>) message
									.getHeaders().get(XdHeaders.XD_HISTORY);
							if (history == null) {
								history = new ArrayList<Map<String, Object>>(1);
							}
							else {
								history = new ArrayList<Map<String, Object>>(history);
							}
							Map<String, Object> map = new LinkedHashMap<String, Object>();
							map.putAll(historyProps);
							map.put("thread", Thread.currentThread().getName());
							history.add(map);
							Message<?> out = messageBuilderFactory.fromMessage(message)
									.setHeader(XdHeaders.XD_HISTORY, history).build();
							return out;
						}
					});
		}
	}
}
