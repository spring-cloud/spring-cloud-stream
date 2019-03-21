/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

/**
 * @author Marius Bogoevici
 */
public interface BinderFactory {

	/**
	 * Returns the binder instance associated with the given configuration name. Instance
	 * caching is a requirement, and implementations must return the same instance on
	 * subsequent invocations with the same arguments.
	 * @param <T> the primary binding type
	 * @param configurationName the name of a binder configuration
	 * @param bindableType binding target type
	 * @return the binder instance
	 */
	<T> Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties> getBinder(
			String configurationName, Class<? extends T> bindableType);

}
