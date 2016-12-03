/*
 * Copyright 2015 the original author or authors.
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

import java.util.Set;

/**
 * Marker interface for instances that can bind/unbind groups of inputs and outputs.
 *
 * Intended for internal use.
 *
 * @author Marius Bogoevici
 */
public interface Bindable {

	/**
	 * Binds all the inputs associated with this instance.
	 */
	void bindInputs(BindingService adapter);

	/**
	 * Binds all the outputs associated with this instance.
	 */
	void bindOutputs(BindingService adapter);

	/**
	 * Unbinds all the inputs associated with this instance.
	 */
	void unbindInputs(BindingService adapter);

	/**
	 * Unbinds all the outputs associated with this instance.
	 */
	void unbindOutputs(BindingService adapter);

	/**
	 * Enumerates all the input binding names.
	 */
	Set<String> getInputs();

	/**
	 * Enumerates all the output binding names.
	 */
	Set<String> getOutputs();

}
