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

package org.springframework.cloud.stream.binding;

import org.springframework.beans.BeansException;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.config.ChannelBindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Dispatches the binding of input/output channels.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@ManagedResource
public class ChannelBindingAdapter implements SmartLifecycle, ApplicationContextAware {

	private Binder<MessageChannel> binder;

	private boolean running = false;

	private Object lifecycleMonitor = new Object();

	private ChannelBindingProperties channelBindingProperties;

	private ConfigurableApplicationContext applicationContext;

	public ChannelBindingAdapter(ChannelBindingProperties channelBindingProperties,
			Binder<MessageChannel> binder) {
		this.channelBindingProperties = channelBindingProperties;
		this.binder = binder;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	public void bindMessageConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingProperties
				.getBindingPath(inputChannelName);
		if (isChannelPubSub(inputChannelName)) {
			this.binder.bindPubSubConsumer(channelBindingTarget, inputChannel,
					this.channelBindingProperties.getConsumerProperties());
		}
		else {
			this.binder.bindConsumer(channelBindingTarget, inputChannel,
					this.channelBindingProperties.getConsumerProperties());
		}
	}

	public void bindMessageProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingProperties
				.getBindingPath(outputChannelName);
		if (isChannelPubSub(outputChannelName)) {
			this.binder.bindPubSubProducer(channelBindingTarget, outputChannel,
					this.channelBindingProperties.getProducerProperties());
		}
		else {
			this.binder.bindProducer(channelBindingTarget, outputChannel,
					this.channelBindingProperties.getProducerProperties());
		}
	}

	private boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName),
				"Channel name should not be empty/null.");
		return channelName.startsWith("topic:");
	}

	public void unbindConsumers(String inputChannelName) {
		this.binder.unbindProducers(inputChannelName);
	}

	public void unbindProducers(String outputChannelName) {
		this.binder.unbindProducers(outputChannelName);
	}

	@Override
	public void start() {
		if (!running) {
			synchronized (lifecycleMonitor) {
				if (!running) {
					BindingUtils.bindAll(this.applicationContext);
					this.running = true;
				}
			}
		}
	}

	@Override
	public void stop() {
		if (running) {
			synchronized (lifecycleMonitor) {
				if (running) {
					BindingUtils.unbindAll(this.applicationContext);
					this.running = false;
				}
			}
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	/**
	 * Return the lowest value to start this bean before any message producing lifecycle
	 * beans.
	 */
	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}

}
