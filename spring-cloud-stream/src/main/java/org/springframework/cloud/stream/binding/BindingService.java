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
import java.util.Collections;
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
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.beanvalidation.CustomValidatorBean;

/**
 * Handles binding of input/output targets by delegating to an underlying {@link Binder}.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class BindingService {

	private final CustomValidatorBean validator;

	private final Log log = LogFactory.getLog(BindingService.class);

	private final BindingServiceProperties bindingServiceProperties;

	private final Map<String, Binding<?>> producerBindings = new HashMap<>();

	private final Map<String, List<Binding<?>>> consumerBindings = new HashMap<>();

	private BinderFactory binderFactory;

	public BindingService(
			BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.binderFactory = binderFactory;
		this.validator = new CustomValidatorBean();
		this.validator.afterPropertiesSet();
	}

	@SuppressWarnings("unchecked")
	public <T> Collection<Binding<T>> bindConsumer(T input, String inputName) {
		String bindingTarget = this.bindingServiceProperties
				.getBindingDestination(inputName);
		String[] bindingTargets = StringUtils
				.commaDelimitedListToStringArray(bindingTarget);
		Collection<Binding<T>> bindings = new ArrayList<>();
		Binder<T, ConsumerProperties, ?> binder = (Binder<T, ConsumerProperties, ?>) getBinder(
				inputName, input.getClass());
		ConsumerProperties consumerProperties = this.bindingServiceProperties
				.getConsumerProperties(inputName);
		if (binder instanceof ExtendedPropertiesBinder) {
			Object extension = ((ExtendedPropertiesBinder) binder)
					.getExtendedConsumerProperties(inputName);
			ExtendedConsumerProperties extendedConsumerProperties = new ExtendedConsumerProperties(
					extension);
			BeanUtils.copyProperties(consumerProperties, extendedConsumerProperties);
			consumerProperties = extendedConsumerProperties;
		}
		validate(consumerProperties);
		for (String target : bindingTargets) {
			Binding<T> binding = binder.bindConsumer(target,
					bindingServiceProperties.getGroup(inputName), input,
					consumerProperties);
			bindings.add(binding);
		}
		bindings = Collections.unmodifiableCollection(bindings);
		this.consumerBindings.put(inputName, new ArrayList<Binding<?>>(bindings));
		return bindings;
	}

	@SuppressWarnings("unchecked")
	public <T> Binding<T> bindProducer(T output, String outputName) {
		String bindingTarget = this.bindingServiceProperties
				.getBindingDestination(outputName);
		Binder<T, ?, ProducerProperties> binder = (Binder<T, ?, ProducerProperties>) getBinder(
				outputName, output.getClass());
		ProducerProperties producerProperties = this.bindingServiceProperties
				.getProducerProperties(outputName);
		if (binder instanceof ExtendedPropertiesBinder) {
			Object extension = ((ExtendedPropertiesBinder) binder)
					.getExtendedProducerProperties(outputName);
			ExtendedProducerProperties extendedProducerProperties = new ExtendedProducerProperties<>(
					extension);
			BeanUtils.copyProperties(producerProperties, extendedProducerProperties);
			producerProperties = extendedProducerProperties;
		}
		validate(producerProperties);
		Binding<T> binding = binder.bindProducer(bindingTarget, output,
				producerProperties);
		this.producerBindings.put(outputName, binding);
		return binding;
	}

	public void unbindConsumers(String inputName) {
		List<Binding<?>> bindings = this.consumerBindings.remove(inputName);
		if (bindings != null && !CollectionUtils.isEmpty(bindings)) {
			for (Binding<?> binding : bindings) {
				binding.unbind();
			}
		}
		else if (log.isWarnEnabled()) {
			log.warn("Trying to unbind '" + inputName + "', but no binding found.");
		}
	}

	public void unbindProducers(String outputName) {
		Binding<?> binding = this.producerBindings.remove(outputName);
		if (binding != null) {
			binding.unbind();
		}
		else if (log.isWarnEnabled()) {
			log.warn("Trying to unbind '" + outputName + "', but no binding found.");
		}
	}

	@SuppressWarnings("unchecked")
	private <T> Binder<T, ?, ?> getBinder(String channelName, Class<T> bindableType) {
		String binderConfigurationName = this.bindingServiceProperties.getBinder(channelName);
		return binderFactory.getBinder(binderConfigurationName, bindableType);
	}

	/**
	 * Provided for backwards compatibility. Will be removed in a future version.
	 *
	 * @return
	 */
	@Deprecated
	public BindingServiceProperties getChannelBindingServiceProperties() {
		return this.bindingServiceProperties;
	}

	public BindingServiceProperties getBindingServiceProperties() {
		return this.bindingServiceProperties;
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
