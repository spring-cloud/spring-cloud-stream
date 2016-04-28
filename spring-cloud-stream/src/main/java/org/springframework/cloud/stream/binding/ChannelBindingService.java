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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.boot.bind.RelaxedDataBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.beanvalidation.CustomValidatorBean;

/**
 * Handles the operations related to channel binding including binding of input/output channels by delegating
 * to an underlying {@link Binder}, setting up data type conversion for binding channel.
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class ChannelBindingService {

	private final CustomValidatorBean validator;

	private final Log log = LogFactory.getLog(ChannelBindingService.class);

	private BinderFactory<MessageChannel> binderFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private final Map<String, Binding<MessageChannel>> producerBindings = new HashMap<>();

	private final Map<String, List<Binding<MessageChannel>>> consumerBindings = new HashMap<>();

	public ChannelBindingService(ChannelBindingServiceProperties channelBindingServiceProperties,
			BinderFactory<MessageChannel> binderFactory) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.binderFactory = binderFactory;
		this.validator = new CustomValidatorBean();
		this.validator.afterPropertiesSet();
	}


	@SuppressWarnings("unchecked")
	public Collection<Binding<MessageChannel>> bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		String[] channelBindingTargets = StringUtils.commaDelimitedListToStringArray(channelBindingTarget);
		List<Binding<MessageChannel>> bindings = new ArrayList<>();
		Binder<MessageChannel, ConsumerProperties, ?> binder =
				(Binder<MessageChannel, ConsumerProperties, ?>) getBinderForChannel(inputChannelName);
		ConsumerProperties consumerProperties =
				this.channelBindingServiceProperties.getConsumerProperties(inputChannelName);
		if (binder instanceof ExtendedPropertiesBinder) {
			Object extension = ((ExtendedPropertiesBinder) binder).getExtendedConsumerProperties(inputChannelName);
			ExtendedConsumerProperties extendedConsumerProperties = new ExtendedConsumerProperties(extension);
			BeanUtils.copyProperties(consumerProperties, extendedConsumerProperties);
			consumerProperties = extendedConsumerProperties;
		}
		validate(consumerProperties);
		for (String target : channelBindingTargets) {
			Binding<MessageChannel> binding = binder.bindConsumer(target, channelBindingServiceProperties.getGroup(inputChannelName), inputChannel, consumerProperties);
			bindings.add(binding);
		}
		this.consumerBindings.put(inputChannelName, bindings);
		return bindings;
	}

	@SuppressWarnings("unchecked")
	public Binding<MessageChannel> bindProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(outputChannelName);
		Binder<MessageChannel, ?, ProducerProperties> binder =
				(Binder<MessageChannel, ?, ProducerProperties>) getBinderForChannel(outputChannelName);
		ProducerProperties producerProperties = this.channelBindingServiceProperties.getProducerProperties(outputChannelName);
		if (binder instanceof ExtendedPropertiesBinder) {
			Object extension = ((ExtendedPropertiesBinder) binder).getExtendedProducerProperties(outputChannelName);
			ExtendedProducerProperties extendedProducerProperties = new ExtendedProducerProperties<>(extension);
			BeanUtils.copyProperties(producerProperties, extendedProducerProperties);
			producerProperties = extendedProducerProperties;
		}
		validate(producerProperties);
		Binding<MessageChannel> binding = binder.bindProducer(channelBindingTarget, outputChannel, producerProperties);
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

	private Binder<MessageChannel, ?, ?> getBinderForChannel(String channelName) {
		String transport = this.channelBindingServiceProperties.getBinder(channelName);
		return binderFactory.getBinder(transport);
	}

	public ChannelBindingServiceProperties getChannelBindingServiceProperties() {
		return this.channelBindingServiceProperties;
	}

	private void validate(Object properties) {
		RelaxedDataBinder dataBinder = new RelaxedDataBinder(properties);
		dataBinder.setValidator(validator);
		dataBinder.validate();
		if (dataBinder.getBindingResult().hasErrors()) {
			throw new IllegalStateException(dataBinder.getBindingResult().toString());
		}
	}
}
