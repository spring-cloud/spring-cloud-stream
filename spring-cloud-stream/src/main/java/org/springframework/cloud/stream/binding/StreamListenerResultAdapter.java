/*
 * Copyright 2016 the original author or authors.
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

import java.io.Closeable;

/**
 * A strategy for adapting the result of a
 * {@link org.springframework.cloud.stream.annotation.StreamListener} annotated method to
 * a binding target annotated with
 * {@link org.springframework.cloud.stream.annotation.Output}.
 *
 * Used when the {@link org.springframework.cloud.stream.annotation.StreamListener}
 * annotated method is operating in declarative mode.
 * @author Marius Bogoevici
 */
public interface StreamListenerResultAdapter<R, B> {

	/**
	 * Return true if the result type can be converted to the binding target.
	 * @param resultType the result type.
	 * @param bindingTarget the binding target.
	 * @return true if the conversion can take place.
	 */
	boolean supports(Class<?> resultType, Class<?> bindingTarget);

	/**
	 * Adapts the result to the binding target.
	 * @param streamListenerResult the result of invoking the method.
	 * @param bindingTarget the binding target.
	 */
	Closeable adapt(R streamListenerResult, B bindingTarget);

}
