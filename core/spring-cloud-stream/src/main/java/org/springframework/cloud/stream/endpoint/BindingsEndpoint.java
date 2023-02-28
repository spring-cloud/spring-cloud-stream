/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.List;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;

/**
 *
 * Actuator endpoint for binding control.
 *
 * @author Oleg Zhurakousky
 * @since 2.0
 */
@Endpoint(id = "bindings")
public class BindingsEndpoint {

	private final BindingsLifecycleController lifecycleController;

	public BindingsEndpoint(BindingsLifecycleController lifecycleController) {
		this.lifecycleController = lifecycleController;
	}

	@WriteOperation
	public void changeState(@Selector String name, State state) {
		this.lifecycleController.changeState(name, state);
	}

	@ReadOperation
	public List<?> queryStates() {
		return this.lifecycleController.queryStates();
	}

	@ReadOperation
	public List<Binding<?>> queryState(@Selector String name) {
		return this.lifecycleController.queryState(name);
	}

}
