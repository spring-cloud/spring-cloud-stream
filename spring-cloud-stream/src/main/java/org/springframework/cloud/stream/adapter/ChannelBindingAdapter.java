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

package org.springframework.cloud.stream.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.config.ChannelBindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.ChannelInterceptorAware;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.channel.BeanFactoryChannelResolver;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.cloud.stream.binder.Binder;

/**
 * Binds input/output channels.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@ManagedResource
public class ChannelBindingAdapter implements Lifecycle, ApplicationContextAware {

	private static Logger logger = LoggerFactory.getLogger(ChannelBindingAdapter.class);

	private Binder binder;
	private MessageBuilderFactory messageBuilderFactory = new DefaultMessageBuilderFactory();

	private Collection<OutputChannelBinding> outputChannels = Collections.emptySet();
	private Collection<InputChannelBinding> inputChannels = Collections.emptySet();

	private boolean running = false;

	private final AtomicBoolean active = new AtomicBoolean(false);

	private boolean trackHistory = false;

	private ChannelBindingProperties module;

	private ConfigurableApplicationContext applicationContext;

	private ChannelLocator channelLocator;

	private DestinationResolver<MessageChannel> channelResolver;

	private Map<String, String> bindings = new HashMap<String, String>();

	public ChannelBindingAdapter(ChannelBindingProperties module, Binder binder) {
		this.module = module;
		this.binder = binder;
		this.channelLocator = new DefaultChannelLocator(module);
	}

	public void setChannelLocator(ChannelLocator channelLocator) {
		this.channelLocator = channelLocator;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		this.channelResolver = new BeanFactoryChannelResolver(applicationContext);
	}

	public void setChannelResolver(DestinationResolver<MessageChannel> channelResolver) {
		this.channelResolver = channelResolver;
	}

	public void setMessageBuilderFactory(MessageBuilderFactory messageBuilderFactory) {
		this.messageBuilderFactory = messageBuilderFactory;
	}

	public void setTrackHistory(boolean trackHistory) {
		this.trackHistory = trackHistory;
	}

	public void setOutputChannels(Collection<OutputChannelBinding> outputChannels) {
		this.outputChannels = new LinkedHashSet<OutputChannelBinding>(outputChannels);
	}

	public void setInputChannels(Collection<InputChannelBinding> inputChannels) {
		this.inputChannels = new LinkedHashSet<InputChannelBinding>(inputChannels);
	}

	public ChannelsMetadata getChannelsMetadata() {
		ChannelsMetadata channels = new ChannelsMetadata();
		channels.setModule(this.module);
		channels.setInputChannels(new LinkedHashSet<InputChannelBinding>(this.inputChannels));
		channels.setOutputChannels(new LinkedHashSet<OutputChannelBinding>(this.outputChannels));
		return channels;
	}

	public OutputChannelBinding getOutputChannel(String name) {
		if (name == null) {
			return null;
		}
		for (OutputChannelBinding binding : this.outputChannels) {
			if (name.equals(binding.getRemoteName())) {
				return binding;
			}
		}
		for (OutputChannelBinding binding : this.outputChannels) {
			if (name.equals(binding.getLocalName())) {
				return binding;
			}
		}
		return null;
	}

	public InputChannelBinding getInputChannel(String name) {
		if (name == null) {
			return null;
		}
		for (InputChannelBinding binding : this.inputChannels) {
			if (name.equals(binding.getRemoteName())) {
				return binding;
			}
		}
		for (InputChannelBinding binding : this.inputChannels) {
			if (name.equals(binding.getLocalName())) {
				return binding;
			}
		}
		return null;
	}

	public void tap(String outputChannel) {
		OutputChannelBinding channel = getOutputChannel(outputChannel);
		if (channel == null || channel.isTapped()) {
			return;
		}
		createAndBindTapChannel(channel.getTapChannelName(), channel.getLocalName());
		channel.setTapped(true);
	}

	public void untap(String outputChannel) {
		OutputChannelBinding channel = getOutputChannel(outputChannel);
		if (channel == null || !channel.isTapped()) {
			return;
		}
		String tapChannelName = channel.getTapChannelName();
		this.binder.unbindProducers(tapChannelName);
		channel.setTapped(false);
	}

	@ManagedOperation
	public void rebind() {
		boolean runnable = locateChannels();
		if (runnable && !this.running) {
			start();
		}
		if (!runnable && this.running) {
			stop();
		}
	}

	@Override
	@ManagedOperation
	public void start() {
		if (!this.running) {
			// Start everything, but don't call ourselves
			if (!this.active.get()) {
				if (this.active.compareAndSet(false, true)) {
					boolean ready = bindChannels();
					if (ready) {
						this.running = true;
						this.applicationContext.start();
					}
					this.active.set(false);
				}
			}
		}
	}

	@Override
	@ManagedOperation
	public void stop() {
		if (this.running) {
			if (!this.active.get()) {
				if (this.active.compareAndSet(false, true)) {
					unbindChannels();
					this.applicationContext.stop();
					this.active.set(false);
				}
			}
		}
		this.running = false;
	}

	@Override
	@ManagedAttribute
	public boolean isRunning() {
		return this.running && this.applicationContext.isRunning();
	}

	protected final void unbindChannels() {
		for (InputChannelBinding binding : this.inputChannels) {
			String name = this.bindings.get(binding.getRemoteName());
			if (name == null) {
				continue;
			}
			this.binder.unbindConsumers(name);
		}
		for (OutputChannelBinding binding : this.outputChannels) {
			String name = this.bindings.get(binding.getRemoteName());
			if (name == null) {
				continue;
			}
			this.binder.unbindProducers(name);
			if (binding.isTapped()) {
				String tapChannelName = binding.getTapChannelName();
				this.binder.unbindProducers(tapChannelName);
			}
		}
	}

	protected final boolean bindChannels() {
		if (!locateChannels()) {
			return false;
		}
		Map<String, Object> historyProperties = new LinkedHashMap<String, Object>();
		if (this.trackHistory) {
			// TODO: addHistoryTag();
		}
		for (OutputChannelBinding binding : this.outputChannels) {
			String name = binding.getRemoteName();
			MessageChannel outputChannel = this.channelResolver.resolveDestination(binding.getLocalName());
			bindMessageProducer(outputChannel, name, this.module.getProducerProperties());
			if (binding.isTapped()) {
				String tapChannelName = this.channelLocator.tap(name);
				binding.setTapChannelName(tapChannelName);
				// tappableChannels.put(tapChannelName, outputChannel);
				// if (isTapActive(tapChannelName)) {
				createAndBindTapChannel(tapChannelName, name);
				// }
			}
			if (this.trackHistory) {
				historyProperties.put("outputChannel", name);
				track(outputChannel, historyProperties);
			}
		}
		for (InputChannelBinding binding : this.inputChannels) {
			String name = binding.getRemoteName();
			MessageChannel inputChannel = this.channelResolver.resolveDestination(binding.getLocalName());
			bindMessageConsumer(inputChannel, name, this.module.getConsumerProperties());
			if (this.trackHistory && this.outputChannels.size() != 1) {
				historyProperties.put("inputChannel", name);
				track(inputChannel, historyProperties);
			}
		}
		return true;
	}

	private boolean locateChannels() {
		logger.info("Locating channels");
		boolean located = true;
		for (OutputChannelBinding binding : this.outputChannels) {
			String name = this.channelLocator.locate(binding.getLocalName());
			if (name == null) {
				logger.info("No channel found for: " + binding.getLocalName());
				located = false;
			}
			binding.setRemoteName(name);
			this.bindings.put(binding.getRemoteName(), name);
		}
		for (InputChannelBinding binding : this.inputChannels) {
			String name = this.channelLocator.locate(binding.getLocalName());
			if (name == null) {
				logger.info("No channel found for: " + binding.getLocalName());
				located = false;
			}
			binding.setRemoteName(name);
			this.bindings.put(binding.getRemoteName(), name);
		}
		return located;
	}

	/*
	 * Following methods copied from parent to support the bindChannels() method above
	 */

	private void bindMessageConsumer(MessageChannel inputChannel,
			String inputChannelName, Properties consumerProperties) {
		if (isChannelPubSub(inputChannelName)) {
			this.binder.bindPubSubConsumer(inputChannelName, inputChannel, consumerProperties);
		}
		else {
			this.binder.bindConsumer(inputChannelName, inputChannel, consumerProperties);
		}
	}

	private void bindMessageProducer(MessageChannel outputChannel,
			String outputChannelName, Properties producerProperties) {
		if (isChannelPubSub(outputChannelName)) {
			this.binder.bindPubSubProducer(outputChannelName, outputChannel, producerProperties);
		}
		else {
			this.binder.bindProducer(outputChannelName, outputChannel, producerProperties);
		}
	}

	private boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName), "Channel name should not be empty/null.");
		return (channelName.startsWith("tap:") || channelName.startsWith("topic:"));
	}

	/**
	 * Creates a wiretap on the output channel and binds the tap channel to
	 * {@link org.springframework.cloud.stream.binder.Binder}'s message target.
	 *
	 * @param tapChannelName the name of the tap channel
	 * @param localName the channel to tap
	 */
	private void createAndBindTapChannel(String tapChannelName, String localName) {
		logger.info("creating and binding tap channel for {}", tapChannelName);
		MessageChannel channel = this.channelResolver.resolveDestination(localName);
		if (channel instanceof ChannelInterceptorAware) {
			DirectChannel tapChannel = new DirectChannel();
			tapChannel.setBeanName(tapChannelName + ".tap.bridge");
			this.binder.bindPubSubProducer(tapChannelName, tapChannel, null); // TODO
			// tap
			// producer
			// props
			tapOutputChannel(tapChannel, (ChannelInterceptorAware) channel);
		}
		else {
			if (logger.isDebugEnabled()) {
				logger.debug("output channel is not interceptor aware. Tap will not be created.");
			}
		}
	}

	private MessageChannel tapOutputChannel(MessageChannel tapChannel, ChannelInterceptorAware outputChannel) {
		outputChannel.addInterceptor(new WireTap(tapChannel));
		return tapChannel;
	}

	private void track(MessageChannel channel, final Map<String, Object> historyProps) {
		if (channel instanceof ChannelInterceptorAware) {
			((ChannelInterceptorAware) channel)
			.addInterceptor(new ChannelInterceptorAdapter() {

				@Override
				public Message<?> preSend(Message<?> message, MessageChannel channel) {
					@SuppressWarnings("unchecked")
					Collection<Map<String, Object>> history = (Collection<Map<String, Object>>) message
					.getHeaders().get(BinderHeaders.BINDER_HISTORY);
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
					Message<?> out = ChannelBindingAdapter.this.messageBuilderFactory.fromMessage(message)
							.setHeader(BinderHeaders.BINDER_HISTORY, history).build();
					return out;
				}
			});
		}
	}

}
