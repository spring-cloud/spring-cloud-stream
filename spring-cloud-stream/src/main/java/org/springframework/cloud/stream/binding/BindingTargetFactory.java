/*
 * Copyright 2016-2019 the original author or authors.
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

/**
 * Defines methods to create/configure the binding targets defined by
 * {@link org.springframework.cloud.stream.annotation.EnableBinding}.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public interface BindingTargetFactory {

	/**
	 * Checks whether a specific binding target type can be created by this factory.
	 * @param clazz the binding target type
	 * @return true if the binding target can be created
	 */
	boolean canCreate(Class<?> clazz);

	/**
	 * Create an input binding target that will be bound via a corresponding
	 * {@link org.springframework.cloud.stream.binder.Binder}.
	 * @param name name of the binding target
	 * @return binding target
	 */
	Object createInput(String name);

	/**
	 * Create an output binding target that will be bound via a corresponding
	 * {@link org.springframework.cloud.stream.binder.Binder}.
	 * @param name name of the binding target
	 * @return binding target
	 */
	Object createOutput(String name);

}
