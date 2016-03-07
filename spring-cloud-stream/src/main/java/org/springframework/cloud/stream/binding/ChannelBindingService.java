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

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.core.ResolvableType;
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

	@SuppressWarnings("unchecked")
	public Collection<Binding<MessageChannel>> bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		String[] channelBindingTargets = StringUtils.commaDelimitedListToStringArray(channelBindingTarget);
		List<Binding<MessageChannel>> bindings = new ArrayList<>();
		Binder<MessageChannel, ConsumerProperties, ?> binder =
				(Binder<MessageChannel, ConsumerProperties, ?>) getBinderForChannel(inputChannelName);
		Class<? extends ConsumerProperties> propertiesClass = resolveConsumerPropertiesType(binder);
		ConsumerProperties consumerProperties =
				this.channelBindingServiceProperties.getConsumerProperties(inputChannelName, propertiesClass);
		for (String target : channelBindingTargets) {
			Binding<MessageChannel> binding = binder.bindConsumer(target, channelBindingServiceProperties.getGroup(inputChannelName),
					inputChannel, consumerProperties);
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
		Class<? extends ProducerProperties> propertiesClass = resolveProducerPropertiesType(binder);
		ProducerProperties producerProperties =
				this.channelBindingServiceProperties.getProducerProperties(outputChannelName, propertiesClass);
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


	static Class<? extends ConsumerProperties> resolveConsumerPropertiesType(Binder<?, ?, ?> binder) {
		return resolveTypeForBinder(binder, ConsumerProperties.class);
	}

	static Class<? extends ProducerProperties> resolveProducerPropertiesType(Binder<?, ?, ?> binder) {
		return resolveTypeForBinder(binder, ProducerProperties.class);
	}

	@SuppressWarnings("unchecked")
	static <T> Class<? extends T> resolveTypeForBinder(Binder<?, ?, ?> binder, Class<? extends T> upperBound) {
		Class<? extends T> propertiesClass = null;
		ResolvableType currentType = ResolvableType.forType(binder.getClass());
		while (!Object.class.equals(currentType.getRawClass()) && propertiesClass == null) {
			ResolvableType[] interfaces = currentType.getInterfaces();
			ResolvableType binderResolvableType = null;
			for (ResolvableType interfaceType : interfaces) {
				if (Binder.class.equals(interfaceType.getRawClass())) {
					binderResolvableType = interfaceType;
					break;
				}
			}
			if (binderResolvableType == null) {
				currentType = currentType.getSuperType();
			}
			else {
				ResolvableType[] generics = binderResolvableType.getGenerics();
				for (ResolvableType generic : generics) {
					Class<?> resolvedParameter = generic.resolve();
					if (resolvedParameter != null && upperBound.isAssignableFrom(resolvedParameter)) {
						propertiesClass = (Class<? extends T>) resolvedParameter;
					}
				}
			}
		}
		if (propertiesClass == null) {
			propertiesClass = upperBound;
		}
		return propertiesClass;
	}
}
