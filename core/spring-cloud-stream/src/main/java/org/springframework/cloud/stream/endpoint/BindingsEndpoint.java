/*
 * Copyright 2018-2024 the original author or authors.
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

package org.springframework.cloud.stream.endpoint;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.actuate.endpoint.SanitizableData;
import org.springframework.boot.actuate.endpoint.Sanitizer;
import org.springframework.boot.actuate.endpoint.SanitizingFunction;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 *
 * Actuator endpoint for binding control.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @since 2.0
 */
@Endpoint(id = "bindings")
public class BindingsEndpoint {

	private final BindingsLifecycleController lifecycleController;

	private final Sanitizer sanitizer;

	private final ObjectMapper objectMapper;

	public BindingsEndpoint(BindingsLifecycleController lifecycleController) {
		this(lifecycleController, null, null);
	}

	/**
	 * @param lifecycleController {@link BindingsLifecycleController}
	 * @param sanitizingFunctions list of user provided {@link SanitizingFunction} beans
	 * @param objectMapper from {@link BindingsLifecycleController}
	 * @since 4.1.2
	 */
	public BindingsEndpoint(BindingsLifecycleController lifecycleController,
							@Nullable Iterable<SanitizingFunction> sanitizingFunctions, @Nullable ObjectMapper objectMapper) {
		this.lifecycleController = lifecycleController;
		this.sanitizer = CollectionUtils.isEmpty((Collection<?>) sanitizingFunctions) ? null :
			new Sanitizer(sanitizingFunctions);
		this.objectMapper = objectMapper;
	}

	@WriteOperation
	public void changeState(@Selector String name, State state) {
		this.lifecycleController.changeState(name, state);
	}

	@ReadOperation
	public List<Map<String, Object>> queryStates() {
		List<Map<String, Object>> bindings = this.lifecycleController.queryStates();
		if (this.sanitizer != null) {
			for (Map<String, Object> binding : bindings) {
				sanitizeSensitiveData(binding);
			}
		}
		return bindings;
	}

	@ReadOperation
	@SuppressWarnings("unchecked")
	public List<Binding<?>> queryState(@Selector String name) {
		List<Binding<?>> bindings = this.lifecycleController.queryState(name);
		if (this.sanitizer != null) {
			List<Map<String, Object>> bindingsAsMap = this.objectMapper.convertValue(bindings, List.class);
			for (Map<String, Object> binding : bindingsAsMap) {
				sanitizeSensitiveData(binding);
			}
			// End users will get a list of map that contains information from the underlying Binding.
			return this.objectMapper.convertValue(bindingsAsMap, List.class);
		}
		return bindings;
	}

	@SuppressWarnings("unchecked")
	public void sanitizeSensitiveData(Map<String, Object> binding) {
		for (String key : binding.keySet()) {
			Object value = binding.get(key);
			if (value != null && Map.class.isAssignableFrom(value.getClass())) {
				// Recursive call since we encountered an inner map
				sanitizeSensitiveData((Map<String, Object>) value);
			}
			else {
				SanitizableData sanitizableData = new SanitizableData(null, key, value);
				Object sanitized = this.sanitizer.sanitize(sanitizableData, true);
				binding.put(key, sanitized);
			}
		}
	}

}
