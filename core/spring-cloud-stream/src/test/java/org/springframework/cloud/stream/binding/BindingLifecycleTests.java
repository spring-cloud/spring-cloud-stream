/*
 * Copyright 2018-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Oleg Zhurakousky
 *
 */
@SuppressWarnings("unchecked")
class BindingLifecycleTests {

	@Test
	void inputBindingLifecycle() {
		Map<String, Bindable> bindables = new HashMap<>();

		Bindable bindableWithTwo = new Bindable() {
			public Collection<Binding<Object>> createAndBindInputs(
					BindingService adapter) {
				return Arrays.asList(mock(Binding.class), mock(Binding.class));
			}
		};
		Bindable bindableWithThree = new Bindable() {
			public Collection<Binding<Object>> createAndBindInputs(
					BindingService adapter) {
				return Arrays.asList(mock(Binding.class), mock(Binding.class),
						mock(Binding.class));
			}
		};
		Bindable bindableEmpty = new Bindable() {
		};

		bindables.put("two", bindableWithTwo);
		bindables.put("empty", bindableEmpty);
		bindables.put("three", bindableWithThree);

		InputBindingLifecycle lifecycle = new InputBindingLifecycle(
				mock(BindingService.class), bindables);
		lifecycle.start();

		Collection<Binding<?>> lifecycleInputBindings = (Collection<Binding<?>>) new DirectFieldAccessor(
				lifecycle).getPropertyValue("inputBindings");
		assertThat(lifecycleInputBindings.size() == 5).isTrue();
		lifecycle.stop();
	}

	@Test
	void outputBindingLifecycle() {
		Map<String, Bindable> bindables = new HashMap<>();

		Bindable bindableWithTwo = new Bindable() {
			public Collection<Binding<Object>> createAndBindOutputs(
					BindingService adapter) {
				return Arrays.asList(mock(Binding.class), mock(Binding.class));
			}
		};
		Bindable bindableWithThree = new Bindable() {
			public Collection<Binding<Object>> createAndBindOutputs(
					BindingService adapter) {
				return Arrays.asList(mock(Binding.class), mock(Binding.class),
						mock(Binding.class));
			}
		};
		Bindable bindableEmpty = new Bindable() {
		};

		bindables.put("two", bindableWithTwo);
		bindables.put("empty", bindableEmpty);
		bindables.put("three", bindableWithThree);

		OutputBindingLifecycle lifecycle = new OutputBindingLifecycle(
				mock(BindingService.class), bindables);
		lifecycle.start();

		Collection<Binding<?>> lifecycleOutputBindings = (Collection<Binding<?>>) new DirectFieldAccessor(
				lifecycle).getPropertyValue("outputBindings");
		assertThat(lifecycleOutputBindings.size() == 5).isTrue();
		lifecycle.stop();
	}

}
