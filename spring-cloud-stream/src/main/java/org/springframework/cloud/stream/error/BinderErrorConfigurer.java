/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.error;


import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;

/**
 * Binder implementations should implement their Error Handling strategy using sub classes of this interface.
 * The {@link AbstractBinder} uses lifecycle hooks to call each of the methods describe in this interface.
 * @author Vinicius Carvalho
 *
 * @since 1.3
 */
public interface BinderErrorConfigurer<C extends ConsumerProperties, T> {
	/**
	 * Registers any error infrastructure needed by the binder to handle errors.
	 *
	 * @param binding target binding that error configuration should be applied
	 * @param group binding group
	 * @param consumerProperties
	 */
	void register(Binding<T> binding, String group, C consumerProperties);

	/**
	 * Cleans any infrastructure created on the register method. It's up to an implementation to keep
	 * track of any resources created so it can destroy it later.
	 *
	 * @param binding target binding that error configuration should be applied
	 * @param group binding group
	 * @param consumerProperties
	 */
	void destroy(Binding<T> binding, String group, C consumerProperties);

	/**
	 * Concrete implementations should override this and configure the binding target. Implementations should
	 * keep track of what is being registered and then fetch the proper infrastructure resource to configure it.
	 *
	 * @param binding target binding that error configuration should be applied
	 */
	void configure(Binding<T> binding);
}
