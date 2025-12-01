/*
 * Copyright 2021-present the original author or authors.
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.function.BindableFunctionProxyFactory;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;


/**
 *
 * Lifecycle controller for the bindings.
 * It is registered as a bean and once injected could be used to control the lifecycle f the bindings.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @since 3.x
 */
public class BindingsLifecycleController implements ApplicationContextAware {

	private final List<InputBindingLifecycle> inputBindingLifecycles;

	private final List<OutputBindingLifecycle> outputBindingsLifecycles;

	private final ObjectMapper objectMapper;

	private ApplicationContext applicationContext;

	@SuppressWarnings("unchecked")
	public BindingsLifecycleController(List<InputBindingLifecycle> inputBindingLifecycles,
			List<OutputBindingLifecycle> outputBindingsLifecycles) {

		Assert.notEmpty(inputBindingLifecycles,
				"'inputBindingLifecycles' must not be null or empty");
		this.inputBindingLifecycles = inputBindingLifecycles;
		this.outputBindingsLifecycles = outputBindingsLifecycles;

		JsonMapper.Builder builder = JsonMapper.builder();

		//this.objectMapper = new ObjectMapper(); //see https://github.com/spring-cloud/spring-cloud-stream/issues/2253
		// we need to use ObjectMapper that could not be modified by the user.

		try {
			Class<? extends JacksonModule> javaTimeModuleClass = (Class<? extends JacksonModule>)
					ClassUtils.forName("tools.jackson.datatype.jsr310.JavaTimeModule", ClassUtils.getDefaultClassLoader());
			JacksonModule javaTimeModule = BeanUtils.instantiateClass(javaTimeModuleClass);
			builder.addModule(javaTimeModule);
		}
		catch (ClassNotFoundException ex) {
			// ignore; jackson-datatype-jsr310 not available
		}
		finally {
			this.objectMapper = builder.build();
		}
	}

	/**
	 * Allows to dynamically define a new input binding returning its consumer properties for further customization.
	 * @param <P> the type of consumer properties. For example, if binding derives from Kafka, it will return KafkaConsumerProperties.
	 * @param bindingName the name of the binding.
	 * @return instance of the consumer properties.
	 */
	public <P> P defineInputBinding(String bindingName) {
		BindableFunctionProxyFactory bindingProxyFactory =
				new BindableFunctionProxyFactory(bindingName, 1, 0, this.applicationContext.getBean(StreamFunctionProperties.class), false);
		this.defineBinding(bindingProxyFactory);
		return this.getExtensionProperties(bindingName);
	}

	/**
	 * Allows to dynamically define a new input binding returning its producer properties for further customization.
	 * @param <P> the type of producer properties. For example, if binding derives from Kafka, it will return KafkaProducerProperties.
	 * @param bindingName the name of the binding.
	 * @return instance of the producer properties.
	 */
	public <P> P defineOutputBinding(String bindingName) {
		BindableFunctionProxyFactory bindingProxyFactory =
				new BindableFunctionProxyFactory(bindingName, 0, 1, this.applicationContext.getBean(StreamFunctionProperties.class), false);
		this.defineBinding(bindingProxyFactory);
		return this.getExtensionProperties(bindingName);
	}

	/**
	 * Will return producer or consumer properties for a specified binding. For example, calling `getExtensionProperties("foo-in-0")`
	 * on Kafka binding  will return an instance of KafkaConsumerProperties.
	 * @param <T> type of producer or consumer properties for a specified binding
	 * @param bindingName name of the binding
	 * @return producer or consumer properties
	 */
	public <T> T getExtensionProperties(String bindingName) {
		List<Binding<?>> locateBinding = BindingsLifecycleController.this.locateBinding(bindingName);
		if (!CollectionUtils.isEmpty(locateBinding)) {
			return locateBinding.get(0).getExtension();
		}
		return null;
	}

	/**
	 * Provide an accessor for the custom ObjectMapper created by this controller.
	 * @return {@link ObjectMapper}
	 * @since 4.1.2
	 */
	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	/**
	 * Convenience method to stop the binding with provided `bindingName`.
	 * @param bindingName the name of the binding.
	 */
	public void stop(String bindingName) {
		this.changeState(bindingName, State.STOPPED);
	}

	/**
	 * Convenience method to start the binding with provided `bindingName`.
	 * @param bindingName the name of the binding.
	 */
	public void start(String bindingName) {
		this.changeState(bindingName, State.STARTED);
	}

	/**
	 * Convenience method to pause the binding with provided `bindingName`.
	 * @param bindingName the name of the binding.
	 */
	public void pause(String bindingName) {
		this.changeState(bindingName, State.PAUSED);
	}

	/**
	 * Convenience method to resume the binding with provided `bindingName`.
	 * @param bindingName the name of the binding.
	 */
	public void resume(String bindingName) {
		this.changeState(bindingName, State.RESUMED);
	}

	/**
	 * General purpose method to change the state of the provided binding.
	 * @param bindingName the name of the binding.
	 * @param state the {@link State} you wish to set this binding to
	 */
	public void changeState(String bindingName, State state) {
		var bindingList = BindingsLifecycleController.this.locateBinding(bindingName);
		if (!bindingList.isEmpty()) {
			switch (state) {
				case STARTED -> bindingList.stream().forEach(Binding::start);
				case STOPPED -> bindingList.stream().forEach(Binding::stop);
				case PAUSED -> bindingList.stream().forEach(Binding::pause);
				case RESUMED -> bindingList.stream().forEach(Binding::resume);
				default -> {
				}
			}
		}
	}

	/**
	 * Queries the {@link List} of states for all available bindings. The returned list
	 * consists of {@link Binding} objects which could be further interrogated
	 * using {@link Binding#isPaused()} and {@link Binding#isRunning()}.
	 * @return the list of {@link Binding}s
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> queryStates() {
		List<Binding<?>> bindings = new ArrayList<>(gatherInputBindings());
		bindings.addAll(gatherOutputBindings());
		return this.objectMapper.convertValue(bindings, List.class);
	}

	/**
	 * Queries the individual state of a binding. The returned list
	 * {@link Binding} object could be further interrogated
	 * using {@link Binding#isPaused()} and {@link Binding#isRunning()}.
	 * @return collection of {@link Binding} objects.
	 */
	public List<Binding<?>> queryState(String name) {
		Assert.notNull(name, "'name' must not be null");
		return this.locateBinding(name);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	private void defineBinding(BindableFunctionProxyFactory bindingProxyFactory) {
		bindingProxyFactory.setApplicationContext(this.applicationContext);
		bindingProxyFactory.afterPropertiesSet();

		BindingService bindingService = this.applicationContext.getBean(BindingService.class);

		AbstractBindingLifecycle bindingLifecycle;
		if (bindingProxyFactory.getInputs().size() > 0) {
			bindingProxyFactory.createAndBindInputs(bindingService);
			bindingLifecycle = this.applicationContext.getBean(InputBindingLifecycle.class);
		}
		else {
			bindingProxyFactory.createAndBindOutputs(bindingService);
			bindingLifecycle = this.applicationContext.getBean(OutputBindingLifecycle.class);
		}

		bindingLifecycle.startBindable(bindingProxyFactory);
	}


	/**
	 * Queries for all input {@link Binding}s.
	 * @return the list of input {@link Binding}s
	 */
	@SuppressWarnings("unchecked")
	private List<Binding<?>> gatherInputBindings() {
		List<Binding<?>> inputBindings = new ArrayList<>();
		for (InputBindingLifecycle inputBindingLifecycle : this.inputBindingLifecycles) {
			Collection<Binding<?>> lifecycleInputBindings = (Collection<Binding<?>>) new DirectFieldAccessor(
					inputBindingLifecycle).getPropertyValue("inputBindings");
			inputBindings.addAll(lifecycleInputBindings);
		}
		return inputBindings;
	}

	/**
	 * Queries for all output {@link Binding}s.
	 * @return the list of output {@link Binding}s
	 */
	@SuppressWarnings("unchecked")
	private List<Binding<?>> gatherOutputBindings() {
		List<Binding<?>> outputBindings = new ArrayList<>();
		for (OutputBindingLifecycle inputBindingLifecycle : this.outputBindingsLifecycles) {
			Collection<Binding<?>> lifecycleInputBindings = (Collection<Binding<?>>) new DirectFieldAccessor(
					inputBindingLifecycle).getPropertyValue("outputBindings");
			outputBindings.addAll(lifecycleInputBindings);
		}
		return outputBindings;
	}

	private List<Binding<?>> locateBinding(String name) {
		Stream<Binding<?>> bindings = Stream.concat(this.gatherInputBindings().stream(),
			this.gatherOutputBindings().stream());
		return bindings.filter(binding -> name.equals(binding.getBindingName())).collect(Collectors.toList());
	}

	/**
	 * Binding states.
	 */
	public enum State {

		/**
		 * Started state of a binding.
		 */
		STARTED,

		/**
		 * Stopped state of a binding.
		 */
		STOPPED,

		/**
		 * Paused state of a binding.
		 */
		PAUSED,

		/**
		 * Resumed state of a binding.
		 */
		RESUMED;

	}

}
