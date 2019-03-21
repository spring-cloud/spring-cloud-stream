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

import org.springframework.core.MethodParameter;

/**
 * Strategy for adapting a method argument type annotated with
 * {@link org.springframework.cloud.stream.annotation.Input} or
 * {@link org.springframework.cloud.stream.annotation.Output} from a binding type (e.g.
 * {@link org.springframework.messaging.MessageChannel}) supported by an existing binder.
 *
 * This is a framework extension and is not primarily intended for use by end-users.
 *
 * @param <A> adapter type
 * @param <B> binding result type
 * @author Marius Bogoevici
 */
public interface StreamListenerParameterAdapter<A, B> {

	/**
	 * Return true if the conversion from the binding target type to the argument type is
	 * supported.
	 * @param bindingTargetType the binding target type
	 * @param methodParameter the method parameter for which the conversion is performed
	 * @return true if the conversion is supported
	 */
	boolean supports(Class<?> bindingTargetType, MethodParameter methodParameter);

	/**
	 * Adapts the binding target to the argument type. The result will be passed as
	 * argument to a method annotated with
	 * {@link org.springframework.cloud.stream.annotation.StreamListener} when used for
	 * setting up a pipeline.
	 * @param bindingTarget the binding target
	 * @param parameter the method parameter for which the conversion is performed
	 * @return an instance of the parameter type, which will be passed to the method
	 */
	A adapt(B bindingTarget, MethodParameter parameter);

}
