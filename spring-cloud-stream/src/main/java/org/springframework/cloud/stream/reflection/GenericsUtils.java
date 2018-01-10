/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.reflection;

import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;

/**
 * Internal utilities for handling generics.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public abstract class GenericsUtils {

	/**
	 * For a specific class that implements or extends a parameterized type, return the
	 * parameter of that interface at a given position. For example, for this class:
	 *
	 * <pre>
	 * {@code
	 * class MessageChannelBinder implements Binder<MessageChannel, ?, ?>
	 * }
	 *
	 * <pre>
	 * {@code
	 * getParameterType(MessageChannelBinder.class, Binder.class, 0);
	 * }
	 *
	 * will return {@code Binder}
	 *
	 * @param evaluatedClass the evaluated class
	 * @param interfaceClass the parametrized interface
	 * @param position the position
	 * @return the parameter type if any
	 * @throws IllegalStateException if the evaluated class does not implement the
	 * interface or
	 */
	public static Class<?> getParameterType(Class<?> evaluatedClass, Class<?> interfaceClass, int position) {
		Class<?> bindableType = null;
		Assert.isTrue(interfaceClass.isInterface(), "'interfaceClass' must be an interface");
		if (!interfaceClass.isAssignableFrom(evaluatedClass)) {
			throw new IllegalStateException(evaluatedClass + " does not implement " + interfaceClass);
		}
		ResolvableType currentType = ResolvableType.forType(evaluatedClass);
		while (!Object.class.equals(currentType.getRawClass()) && bindableType == null) {
			ResolvableType[] interfaces = currentType.getInterfaces();
			ResolvableType resolvableType = null;
			for (ResolvableType interfaceType : interfaces) {
				if (interfaceClass.equals(interfaceType.getRawClass())) {
					resolvableType = interfaceType;
					break;
				}
			}
			if (resolvableType == null) {
				currentType = currentType.getSuperType();
			}
			else {
				ResolvableType[] generics = resolvableType.getGenerics();
				ResolvableType generic = generics[position];
				Class<?> resolvedParameter = generic.resolve();
				if (resolvedParameter != null) {
					bindableType = resolvedParameter;
				}
				else {
					bindableType = Object.class;
				}
			}
		}
		if (bindableType == null) {
			throw new IllegalStateException("Cannot find parameter of " + evaluatedClass.getName() + " for "
					+ interfaceClass + " at position " + position);
		}
		return bindableType;
	}
}
