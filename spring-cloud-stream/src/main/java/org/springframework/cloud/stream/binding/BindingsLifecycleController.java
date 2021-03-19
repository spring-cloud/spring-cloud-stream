/*
 * Copyright 2021-2021 the original author or authors.
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
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.util.Assert;

/**
 *
 * Lifecycle controller for the bindings.
 * It is registered as a bean and once injected could be used to control the lifecycle f the bindings.
 *
 * @author Oleg Zhurakousky
 * @since 3.x
 */
public class BindingsLifecycleController {

	private final List<InputBindingLifecycle> inputBindingLifecycles;

	private final List<OutputBindingLifecycle> outputBindingsLifecycles;

	private final ObjectMapper objectMapper;

	public BindingsLifecycleController(List<InputBindingLifecycle> inputBindingLifecycles,
			List<OutputBindingLifecycle> outputBindingsLifecycles) {
		Assert.notEmpty(inputBindingLifecycles,
				"'inputBindingLifecycles' must not be null or empty");
		this.inputBindingLifecycles = inputBindingLifecycles;
		this.outputBindingsLifecycles = outputBindingsLifecycles;
		this.objectMapper = new ObjectMapper();
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
		Binding<?> binding = BindingsLifecycleController.this.locateBinding(bindingName);
		if (binding != null) {
			switch (state) {
			case STARTED:
				binding.start();
				break;
			case STOPPED:
				binding.stop();
				break;
			case PAUSED:
				binding.pause();
				break;
			case RESUMED:
				binding.resume();
				break;
			default:
				break;
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
	public List<Binding<?>> queryStates() {
		List<Binding<?>> bindings = new ArrayList<>(gatherInputBindings());
		bindings.addAll(gatherOutputBindings());
		return this.objectMapper.convertValue(bindings, List.class);
	}

	/**
	 * Queries the individual state of a binding. The returned list
	 * {@link Binding} object could be further interrogated
	 * using {@link Binding#isPaused()} and {@link Binding#isRunning()}.
	 * @param name the name of the binding
	 * @return instance of {@link Binding} object.
	 */
	public Binding<?> queryState(String name) {
		Assert.notNull(name, "'name' must not be null");
		return this.locateBinding(name);
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

	private Binding<?> locateBinding(String name) {
		Stream<Binding<?>> bindings = Stream.concat(this.gatherInputBindings().stream(),
				this.gatherOutputBindings().stream());
		return bindings.filter(binding -> name.equals(binding.getBindingName())).findFirst()
				.orElse(null);
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
