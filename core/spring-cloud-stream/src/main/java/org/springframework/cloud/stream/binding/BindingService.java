/*
 * Copyright 2015-present the original author or authors.
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.Advised;
import org.springframework.beans.BeanUtils;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderWrapper;
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

import static org.springframework.cloud.stream.utils.CacheKeyCreatorUtils.createChannelCacheKey;
import static org.springframework.cloud.stream.utils.CacheKeyCreatorUtils.getBinderNameIfNeeded;

/**
 * Handles binding of input/output targets by delegating to an underlying {@link Binder}.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Janne Valkealahti
 * @author Soby Chacko
 * @author Michael Michailidis
 * @author Chris Bono
 * @author Artem Bilan
 * @author Byungjun You
 * @author Omer Celik
 */
public class BindingService {

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
		this.taskScheduler = taskScheduler;
		this.objectMapper = objectMapper;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> Collection<Binding<T>> bindConsumer(T input, String inputName) {
		Collection<Binding<T>> bindings = new ArrayList<>();
		Class<?> inputClass = input.getClass();
		if (input instanceof Advised advisedInput) {
			inputClass = Stream.of(advisedInput.getProxiedInterfaces()).filter(c -> !c.getName().contains("org.springframework")).findFirst()
					.orElse(inputClass);
		}
		Binder<T, ConsumerProperties, ?> binder = (Binder<T, ConsumerProperties, ?>) getBinder(
				inputName, inputClass);
		ConsumerProperties consumerProperties = this.bindingServiceProperties
				.getConsumerProperties(inputName);
		if (binder instanceof ExtendedPropertiesBinder extendedPropertiesBinder) {
			Object extension = extendedPropertiesBinder
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

						Object extension = consumerProperties instanceof ExtendedConsumerProperties extendedProperties
								? extendedProperties.getExtension()
										: null;

						ConsumerProperties consumerPropertiesTemp = new ExtendedConsumerProperties<>(extension);
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
	public <T> Binding<T> bindProducer(T output, boolean cache, BinderWrapper binderWrapper) {
		ProducerProperties producerProperties = this.bindingServiceProperties
				.getProducerProperties(binderWrapper.destinationName());
		if (binderWrapper.binder() instanceof ExtendedPropertiesBinder extendedPropertiesBinder) {
			Object extension = extendedPropertiesBinder.getExtendedProducerProperties(binderWrapper.destinationName());
			ExtendedProducerProperties extendedProducerProperties = new ExtendedProducerProperties<>(
					extension);
			BeanUtils.copyProperties(producerProperties, extendedProducerProperties);

			producerProperties = extendedProducerProperties;
		}
		producerProperties.populateBindingName(binderWrapper.destinationName());
		validate(producerProperties);
		String bindingTarget = this.bindingServiceProperties.getBindingDestination(binderWrapper.destinationName());
		Binding<T> binding = doBindProducer(output, bindingTarget, binderWrapper.binder(),
				producerProperties);
		// If the downstream binder modified the partition count in the extended producer properties
		// based on the higher number of partitions provisioned on the target middleware, update that
		// in the original producer properties.
		ProducerProperties originalProducerProperties = this.bindingServiceProperties
			.getProducerProperties(binderWrapper.destinationName());
		if (originalProducerProperties.getPartitionCount() < producerProperties.getPartitionCount()) {
			originalProducerProperties.setPartitionCount(producerProperties.getPartitionCount());
		}
		if (cache) {
			this.producerBindings.put(binderWrapper.cacheKey(), binding);
		}
		return binding;
	}

	public <T> Binding<T> bindProducer(T output, String outputName, boolean cache) {
		Class<?> outputClass = output.getClass();
		if (output instanceof Advised advisedOutput) {
			outputClass = Stream.of(advisedOutput.getProxiedInterfaces()).filter(c -> !c.getName().contains("org.springframework")).findFirst()
				.orElse(outputClass);
		}
		BinderWrapper binderWrapper = createBinderWrapper(null, outputName, outputClass);
		return this.bindProducer(output, cache, binderWrapper);
	}

	public <T> Binding<T> bindProducer(T output, String outputName) {
		return this.bindProducer(output, outputName, true);
	}

	@SuppressWarnings("rawtypes")
	public Object getExtendedProducerProperties(Binder binder, String outputName) {
		if (binder instanceof ExtendedPropertiesBinder extendedPropertiesBinder) {
			return extendedPropertiesBinder.getExtendedProducerProperties(outputName);
		}
		return null;
	}

	public String[] getProducerBindingNames() {
		return this.producerBindings.keySet().toArray(new String[] {});
	}

	@Nullable
	public Binding<?> getProducerBinding(String bindingName) {
		return this.producerBindings.get(bindingName);
	}

	public String[] getConsumerBindingNames() {
		return this.consumerBindings.keySet().toArray(new String[] {});
	}

	public List<Binding<?>> getConsumerBindings(String bindingName) {
		return this.consumerBindings.getOrDefault(bindingName, Collections.emptyList());
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

	public void unbindProducers(@Nullable  String binderName, String outputName) {
		String cacheKey = createChannelCacheKey(binderName, outputName, bindingServiceProperties);
		unbindProducers(cacheKey);
	}

	public void unbindProducers(String cacheKey) {
		Binding<?> binding = this.producerBindings.remove(cacheKey);

		if (binding != null) {
			binding.stop();
			//then
			binding.unbind();
		}
		else if (this.log.isWarnEnabled()) {
			this.log.warn("Trying to unbind '" + cacheKey + "', but no binding found.");
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
//		dataBinder.setValidator(this.validator);
		dataBinder.validate();
		if (dataBinder.getBindingResult().hasErrors()) {
			throw new IllegalStateException(dataBinder.getBindingResult().toString());
		}
	}

	private void scheduleTask(Runnable task) {
		this.taskScheduler.schedule(task, Instant.ofEpochMilli(System.currentTimeMillis()
				+ this.bindingServiceProperties.getBindingRetryInterval() * 1_000));
	}

	private void assertNotIllegalException(RuntimeException exception)
			throws RuntimeException {
		if (exception instanceof IllegalStateException
				|| exception instanceof IllegalArgumentException) {
			throw exception;
		}
	}

	public BinderWrapper createBinderWrapper(@Nullable String binderName, String destinationName, Class<?> outputClass) {
		binderName = getBinderNameIfNeeded(binderName, destinationName, bindingServiceProperties);
		Binder binder = binderFactory.getBinder(binderName, outputClass);
		String channelCacheKey = createChannelCacheKey(binderName, destinationName);
		return new BinderWrapper(binder, destinationName, channelCacheKey);
	}


	public static class LateBinding<T> implements Binding<T> {

		private volatile Binding<T> delegate;

		private volatile boolean unbound;

		private final String error;

		private final String bindingName;

		private final Object consumerOrProducerProperties;

		private final boolean isInput;

		final ObjectMapper objectMapper;

		private final ReentrantLock lock = new ReentrantLock();

		LateBinding(String bindingName, String error, Object consumerOrProducerProperties, boolean isInput, ObjectMapper objectMapper) {
			super();
			this.error = error;
			this.bindingName = bindingName;
			this.consumerOrProducerProperties = consumerOrProducerProperties;
			this.isInput = isInput;
			this.objectMapper = objectMapper;
		}

		public void setDelegate(Binding<T> delegate) {
			try {
				this.lock.lock();
				if (this.unbound) {
					delegate.unbind();
				}
				else {
					this.delegate = delegate;
				}
			}
			finally {
				this.lock.unlock();
			}
		}

		@Override
		public void unbind() {
			try {
				this.lock.lock();
				this.unbound = true;
				if (this.delegate != null) {
					this.delegate.unbind();
				}
			}
			finally {
				this.lock.unlock();
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
			extendedInfo.put(consumerOrProducerProperties.getClass().getSimpleName(),
					this.objectMapper.convertValue(consumerOrProducerProperties, Map.class));
			return extendedInfo;
		}

		@Override
		public boolean isInput() {
			return this.isInput;
		}

	}

}
