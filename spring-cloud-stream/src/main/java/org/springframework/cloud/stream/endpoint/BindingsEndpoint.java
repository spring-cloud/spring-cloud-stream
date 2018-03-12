/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.endpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.util.Assert;

/**
 *
 * Actuator endpoint for binding control
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.0
 *
 */
@Endpoint(id = "bindings")
public class BindingsEndpoint {

	private final List<InputBindingLifecycle> inputBindingLifecycles;

	private final ObjectMapper objectMapper;

	public BindingsEndpoint(List<InputBindingLifecycle> inputBindingLifecycles) {
		Assert.notEmpty(inputBindingLifecycles, "'inputBindingLifecycles' must not be null or empty");
		this.inputBindingLifecycles = inputBindingLifecycles;
		this.objectMapper = new ObjectMapper();
	}

	@WriteOperation
	public void changeState(@Selector String name, State state) {
		Binding<?> binding = BindingsEndpoint.this.locateBinding(name);
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

	@ReadOperation
	public List<?> queryStates() {
		return objectMapper.convertValue(gatherInputBindings(), List.class);
	}

	@ReadOperation
	public Binding<?> queryState(@Selector String name) {
		Assert.notNull(name, "'name' must not be null");
		return this.locateBinding(name);
	}

	@SuppressWarnings("unchecked")
	private List<Binding<?>> gatherInputBindings() {
		List<Binding<?>> inputBindings = new ArrayList<>();
		for (InputBindingLifecycle inputBindingLifecycle : this.inputBindingLifecycles) {
			Collection<Binding<?>> lifecycleInputBindings =
					(Collection<Binding<?>>) new DirectFieldAccessor(inputBindingLifecycle).getPropertyValue("inputBindings");
			inputBindings.addAll(lifecycleInputBindings);
		}
		return inputBindings;
	}

	private Binding<?> locateBinding(String name) {
		return BindingsEndpoint.this.gatherInputBindings().stream()
			.filter(binding -> name.equals(binding.getName()))
			.findFirst()
			.orElse(null);
	}

	private enum State {
		STARTED,
		STOPPED,
		PAUSED,
		RESUMED;
	}
}
