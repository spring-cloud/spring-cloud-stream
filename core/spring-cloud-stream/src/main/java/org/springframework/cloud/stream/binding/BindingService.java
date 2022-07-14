/*
 * Copyright 2015-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.Advised;
import org.springframework.beans.BeanUtils;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.PollableConsumerBinder;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.DataBinder;
import org.springframework.validation.beanvalidation.CustomValidatorBean;

/**
 * Handles binding of input/output targets by delegating to an underlying {@link Binder}.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan`
 * @author Gary Russell
 * @author Janne Valkealahti
 * @author Soby Chacko
 * @author Michael Michailidis
 */
public class BindingService {

	private final CustomValidatorBean validator;

	private final Log log = LogFactory.getLog(BindingService.class);

	private final BindingServiceProperties bindingServiceProperties;

	private final Map<String, Binding<?>> producerBindings = new HashMap<>();

	private final Map<String, List<Binding<?>>> consumerBindings = new HashMap<>();

	private final TaskScheduler taskScheduler;

	private final BinderFactory binderFactory;

	private final ObjectMapper objectMapper;

	public BindingService(BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory, ObjectMapper objectMapper) {
		this(bindingServiceProperties, binderFactory, null, objectMapper);
	}

	public BindingService(BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory, TaskScheduler taskScheduler, ObjectMapper objectMapper) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.binderFactory = binderFactory;
		this.validator = new CustomValidatorBean();
		this.validator.afterPropertiesSet();
		this.taskScheduler = taskScheduler;
		this.objectMapper = objectMapper;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> Collection<Binding<T>> bindConsumer(T input, String inputName) {
		Collection<Binding<T>> bindings = new ArrayList<>();
		Class<?> inputClass = input.getClass();
		if (input instanceof Advised) {
			inputClass = Stream.of(((Advised) input).getProxiedInterfaces()).filter(c -> !c.getName().contains("org.springframework")).findFirst()
					.orElse(inputClass);
		}
		Binder<T, ConsumerProperties, ?> binder = (Binder<T, ConsumerProperties, ?>) getBinder(
				inputName, inputClass);
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
		consumerProperties.populateBindingName(inputName);
		validate(consumerProperties);

		String bindingTarget = this.bindingServiceProperties
				.getBindingDestination(inputName);

		if (consumerProperties.isMultiplex()) {
			bindings.add(doBindConsumer(input, inputName, binder, consumerProperties,
					bindingTarget));
		}
		else {
			String[] bindingTargets = StringUtils
					.commaDelimitedListToStringArray(bindingTarget);
			for (String target : bindingTargets) {
				if (!consumerProperties.isPartitioned() || consumerProperties.getInstanceIndexList().isEmpty()) {
					Binding<T> binding = input instanceof PollableSource
						? doBindPollableConsumer(input, inputName, binder,
						consumerProperties, target)
						: doBindConsumer(input, inputName, binder, consumerProperties,
						target);

					bindings.add(binding);
				}
				else {
					for (Integer index : consumerProperties.getInstanceIndexList()) {
						if (index < 0) {
							continue;
						}

						Object extensiion = consumerProperties instanceof ExtendedConsumerProperties
								? ((ExtendedConsumerProperties) consumerProperties).getExtension()
										: null;

						ConsumerProperties consumerPropertiesTemp = new ExtendedConsumerProperties<>(extensiion);
						BeanUtils.copyProperties(consumerProperties, consumerPropertiesTemp);

						consumerPropertiesTemp.setInstanceIndex(index);

						Binding<T> binding = input instanceof PollableSource
							? doBindPollableConsumer(input, inputName, binder,
							consumerPropertiesTemp, target)
							: doBindConsumer(input, inputName, binder, consumerPropertiesTemp,
							target);

						bindings.add(binding);
					}
				}
			}
		}
		bindings = Collections.unmodifiableCollection(bindings);
		this.consumerBindings.put(inputName, new ArrayList<>(bindings));
		return bindings;
	}

	public <T> Binding<T> doBindConsumer(T input, String inputName,
			Binder<T, ConsumerProperties, ?> binder,
			ConsumerProperties consumerProperties, String target) {
		if (this.taskScheduler == null
				|| this.bindingServiceProperties.getBindingRetryInterval() <= 0) {
			return binder.bindConsumer(target,
					this.bindingServiceProperties.getGroup(inputName), input,
					consumerProperties);
		}
		else {
			try {
				return binder.bindConsumer(target,
						this.bindingServiceProperties.getGroup(inputName), input,
						consumerProperties);
			}
			catch (RuntimeException e) {
				LateBinding<T> late = new LateBinding<T>(target,
						e.getCause() == null ? e.toString() : e.getCause().getMessage(), consumerProperties, true, this.objectMapper);
				rescheduleConsumerBinding(input, inputName, binder, consumerProperties,
						target, late, e);
				this.consumerBindings.put(inputName, Collections.singletonList(late));
				return late;
			}
		}
	}

	public <T> void rescheduleConsumerBinding(final T input, final String inputName,
			final Binder<T, ConsumerProperties, ?> binder,
			final ConsumerProperties consumerProperties, final String target,
			final LateBinding<T> late, RuntimeException exception) {
		assertNotIllegalException(exception);
		this.log.error("Failed to create consumer binding; retrying in "
				+ this.bindingServiceProperties.getBindingRetryInterval() + " seconds",
				exception);
		this.scheduleTask(() -> {
			try {
				late.setDelegate(binder.bindConsumer(target,
						this.bindingServiceProperties.getGroup(inputName), input,
						consumerProperties));
			}
			catch (RuntimeException e) {
				rescheduleConsumerBinding(input, inputName, binder, consumerProperties,
						target, late, e);
			}
		});
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> Binding<T> doBindPollableConsumer(T input, String inputName,
			Binder<T, ConsumerProperties, ?> binder,
			ConsumerProperties consumerProperties, String target) {
		if (this.taskScheduler == null
				|| this.bindingServiceProperties.getBindingRetryInterval() <= 0) {
			return ((PollableConsumerBinder) binder).bindPollableConsumer(target,
					this.bindingServiceProperties.getGroup(inputName),
					(PollableSource) input, consumerProperties);
		}
		else {
			try {
				return ((PollableConsumerBinder) binder).bindPollableConsumer(target,
						this.bindingServiceProperties.getGroup(inputName),
						(PollableSource) input, consumerProperties);
			}
			catch (RuntimeException e) {
				LateBinding<T> late = new LateBinding<T>(target,
						e.getCause() == null ? e.toString() : e.getCause().getMessage(), consumerProperties, true, this.objectMapper);
				reschedulePollableConsumerBinding(input, inputName, binder,
						consumerProperties, target, late, e);
				return late;
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> void reschedulePollableConsumerBinding(final T input,
			final String inputName, final Binder<T, ConsumerProperties, ?> binder,
			final ConsumerProperties consumerProperties, final String target,
			final LateBinding<T> late, RuntimeException exception) {
		assertNotIllegalException(exception);
		this.log.error("Failed to create consumer binding; retrying in "
				+ this.bindingServiceProperties.getBindingRetryInterval() + " seconds",
				exception);
		this.scheduleTask(() -> {
			try {
				late.setDelegate(((PollableConsumerBinder) binder).bindPollableConsumer(
						target, this.bindingServiceProperties.getGroup(inputName),
						(PollableSource) input, consumerProperties));
			}
			catch (RuntimeException e) {
				reschedulePollableConsumerBinding(input, inputName, binder,
						consumerProperties, target, late, e);
			}
		});
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> Binding<T> bindProducer(T output, String outputName, boolean cache, @Nullable Binder<T, ?, ProducerProperties> binder) {
		String bindingTarget = this.bindingServiceProperties.getBindingDestination(outputName);
		Class<?> outputClass = output.getClass();
		if (output instanceof Advised) {
			outputClass = Stream.of(((Advised) output).getProxiedInterfaces()).filter(c -> !c.getName().contains("org.springframework")).findFirst()
					.orElse(outputClass);
		}
		if (binder == null) {
			binder = (Binder<T, ?, ProducerProperties>) getBinder(outputName, outputClass);
		}

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
		producerProperties.populateBindingName(outputName);
		validate(producerProperties);
		Binding<T> binding = doBindProducer(output, bindingTarget, binder,
				producerProperties);
		if (cache) {
			this.producerBindings.put(outputName, binding);
		}
		return binding;
	}

	public <T> Binding<T> bindProducer(T output, String outputName, boolean cache) {
		return this.bindProducer(output, outputName, cache, null);
	}

	public <T> Binding<T> bindProducer(T output, String outputName) {
		return this.bindProducer(output, outputName, true);
	}

	@SuppressWarnings("rawtypes")
	public Object getExtendedProducerProperties(Object output, String outputName) {
		Binder binder = getBinder(outputName, output.getClass());
		if (binder instanceof ExtendedPropertiesBinder) {
			return ((ExtendedPropertiesBinder) binder)
					.getExtendedProducerProperties(outputName);
		}
		return null;
	}

	public <T> Binding<T> doBindProducer(T output, String bindingTarget,
			Binder<T, ?, ProducerProperties> binder,
			ProducerProperties producerProperties) {
		if (this.taskScheduler == null
				|| this.bindingServiceProperties.getBindingRetryInterval() <= 0) {
			return binder.bindProducer(bindingTarget, output, producerProperties);
		}
		else {
			try {
				return binder.bindProducer(bindingTarget, output, producerProperties);
			}
			catch (RuntimeException e) {
				LateBinding<T> late = new LateBinding<T>(bindingTarget,
						e.getCause() == null ? e.toString() : e.getCause().getMessage(), producerProperties, false, this.objectMapper);
				rescheduleProducerBinding(output, bindingTarget, binder,
						producerProperties, late, e);
				return late;
			}
		}
	}

	public <T> void rescheduleProducerBinding(final T output, final String bindingTarget,
			final Binder<T, ?, ProducerProperties> binder,
			final ProducerProperties producerProperties, final LateBinding<T> late,
			final RuntimeException exception) {
		assertNotIllegalException(exception);
		this.log.error("Failed to create producer binding; retrying in "
				+ this.bindingServiceProperties.getBindingRetryInterval() + " seconds",
				exception);
		this.scheduleTask(() -> {
			try {
				late.setDelegate(
						binder.bindProducer(bindingTarget, output, producerProperties));
			}
			catch (RuntimeException e) {
				rescheduleProducerBinding(output, bindingTarget, binder,
						producerProperties, late, e);
			}
		});
	}

	public void unbindConsumers(String inputName) {
		List<Binding<?>> bindings = this.consumerBindings.remove(inputName);
		if (bindings != null && !CollectionUtils.isEmpty(bindings)) {
			for (Binding<?> binding : bindings) {
				binding.stop();
				//then
				binding.unbind();
			}
		}
		else if (this.log.isWarnEnabled()) {
			this.log.warn("Trying to unbind '" + inputName + "', but no binding found.");
		}
	}

	public void unbindProducers(String outputName) {
		Binding<?> binding = this.producerBindings.remove(outputName);

		if (binding != null) {
			binding.stop();
			//then
			binding.unbind();
		}
		else if (this.log.isWarnEnabled()) {
			this.log.warn("Trying to unbind '" + outputName + "', but no binding found.");
		}
	}

	public BindingServiceProperties getBindingServiceProperties() {
		return this.bindingServiceProperties;
	}

	protected <T> Binder<T, ?, ?> getBinder(String channelName, Class<T> bindableType) {
		String binderConfigurationName = this.bindingServiceProperties
				.getBinder(channelName);
		return this.binderFactory.getBinder(binderConfigurationName, bindableType);
	}

	private void validate(Object properties) {
		DataBinder dataBinder = new DataBinder(properties);
		dataBinder.setValidator(this.validator);
		dataBinder.validate();
		if (dataBinder.getBindingResult().hasErrors()) {
			throw new IllegalStateException(dataBinder.getBindingResult().toString());
		}
	}

	private void scheduleTask(Runnable task) {
		Date d = new Date(System.currentTimeMillis()
				+ this.bindingServiceProperties.getBindingRetryInterval() * 1_000);
		this.taskScheduler.schedule(task, d.toInstant());
	}

	private void assertNotIllegalException(RuntimeException exception)
			throws RuntimeException {
		if (exception instanceof IllegalStateException
				|| exception instanceof IllegalArgumentException) {
			throw exception;
		}
	}

	public static class LateBinding<T> implements Binding<T> {

		private volatile Binding<T> delegate;

		private volatile boolean unbound;

		private final String error;

		private final String bindingName;

		private final Object consumerOrProducerproperties;

		private final boolean isInput;

		final ObjectMapper objectMapper;

		LateBinding(String bindingName, String error, Object consumerOrProducerproperties, boolean isInput, ObjectMapper objectMapper) {
			super();
			this.error = error;
			this.bindingName = bindingName;
			this.consumerOrProducerproperties = consumerOrProducerproperties;
			this.isInput = isInput;
			this.objectMapper = objectMapper;
		}

		public synchronized void setDelegate(Binding<T> delegate) {
			if (this.unbound) {
				delegate.unbind();
			}
			else {
				this.delegate = delegate;
			}
		}

		@Override
		public synchronized void unbind() {
			this.unbound = true;
			if (this.delegate != null) {
				this.delegate.unbind();
			}
		}

		@Override
		public String getName() {
			return this.bindingName;
		}

		@Override
		public String getBindingName() {
			return this.bindingName;
		}

		@SuppressWarnings("unused")
		public String getError() {
			return this.error;
		}

		@Override
		public String toString() {
			return "LateBinding [delegate=" + this.delegate + "]";
		}

		@Override
		public Map<String, Object> getExtendedInfo() {
			Map<String, Object> extendedInfo = new LinkedHashMap<>();
			extendedInfo.put("bindingDestination", this.getBindingName());
			extendedInfo.put(consumerOrProducerproperties.getClass().getSimpleName(),
					this.objectMapper.convertValue(consumerOrProducerproperties, Map.class));
			return extendedInfo;
		}

		@Override
		public boolean isInput() {
			return this.isInput;
		}

	}

}
