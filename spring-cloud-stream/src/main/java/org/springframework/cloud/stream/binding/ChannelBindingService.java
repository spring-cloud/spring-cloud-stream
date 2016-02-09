/*
 * Copyright 2015-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Handles the operations related to channel binding including binding of input/output channels by delegating
 * to an underlying {@link Binder}, setting up data type conversion for binding channel.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class ChannelBindingService {

	private final Log log = LogFactory.getLog(ChannelBindingService.class);

	private BinderFactory<MessageChannel> binderFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private final Map<String, Binding<MessageChannel>> producerBindings = new HashMap<>();

	private final Map<String, List<Binding<MessageChannel>>> consumerBindings = new HashMap<>();

	public ChannelBindingService(ChannelBindingServiceProperties channelBindingServiceProperties,
								 BinderFactory<MessageChannel> binderFactory) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.binderFactory = binderFactory;
	}

	public Collection<Binding<MessageChannel>> bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		String[] channelBindingTargets = StringUtils.commaDelimitedListToStringArray(channelBindingTarget);
		List<Binding<MessageChannel>> bindings = new ArrayList<>();

		Binder<MessageChannel> binder = getBinderForChannel(inputChannelName);
		String consumerGroup = consumerGroup(inputChannelName);
		Properties consumerProperties = this.channelBindingServiceProperties.getConsumerProperties(inputChannelName);

		for (String target : channelBindingTargets) {
			Binding<MessageChannel> binding = binder.bindConsumer(target, consumerGroup, inputChannel,
					consumerProperties);
			bindings.add(binding);
		}
		this.consumerBindings.put(inputChannelName, bindings);
		return bindings;
	}

	public Binding<MessageChannel> bindProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(outputChannelName);
		Binder<MessageChannel> binder = getBinderForChannel(outputChannelName);
		Binding<MessageChannel> binding = binder.bindProducer(channelBindingTarget, outputChannel,
				this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
		this.producerBindings.put(outputChannelName, binding);
		return binding;
	}

	public void unbindConsumers(String inputChannelName) {
		List<Binding<MessageChannel>> bindings = this.consumerBindings.remove(inputChannelName);
		if (bindings != null && !CollectionUtils.isEmpty(bindings)) {
			for (Binding<MessageChannel> binding : bindings) {
				binding.unbind();
			}
		}
		else if (log.isWarnEnabled()) {
			log.warn("Trying to unbind channel '" + inputChannelName + "', but no binding found.");
		}
	}

	public void unbindProducers(String outputChannelName) {
		Binding<MessageChannel> binding = this.producerBindings.remove(outputChannelName);
		if (binding != null) {
			binding.unbind();
		}
		else if (log.isWarnEnabled()) {
			log.warn("Trying to unbind channel '" + outputChannelName + "', but no binding found.");
		}
	}

	private Binder<MessageChannel> getBinderForChannel(String channelName) {
		String transport = this.channelBindingServiceProperties.getBinder(channelName);
		return binderFactory.getBinder(transport);
	}

	private String consumerGroup(String inputChannelName) {
		BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindings()
				.get(inputChannelName);
		return bindingProperties == null ? null : bindingProperties.getGroup();
	}

}
