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

import org.springframework.util.Assert;

/**
 * A {@link BindingTargetFactory} implementation that restricts the type of binding target
 * to a specified class and its supertypes.
 *
 * @param <T> type of binding target
 * @author Marius Bogoevici
 */
public abstract class AbstractBindingTargetFactory<T> implements BindingTargetFactory {

	private final Class<T> bindingTargetType;

	protected AbstractBindingTargetFactory(Class<T> bindingTargetType) {
		Assert.notNull(bindingTargetType, "The binding target type cannot be null");
		this.bindingTargetType = bindingTargetType;
	}

	@Override
	public final boolean canCreate(Class<?> clazz) {
		return clazz.isAssignableFrom(this.bindingTargetType);
	}

	@Override
	public abstract T createInput(String name);

	@Override
	public abstract T createOutput(String name);

}
