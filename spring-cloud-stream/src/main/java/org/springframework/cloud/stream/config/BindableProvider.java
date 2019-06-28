/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.config;

/**
 * If downstream binders want to take over the responsibility of binding the target types (such as the Kafka Streams binder),
 * then they can implement this functional interface to signal the core framework to bypass any binding.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
@FunctionalInterface
public interface BindableProvider {

	/**
	 * Based on the type provided, the implementation can determine whether it is capable of
	 * binding this target type.
	 *
	 * @param clazz target type to bind
	 * @return true if capable of binding
	 */
	boolean canBind(Class<?> clazz);
}
