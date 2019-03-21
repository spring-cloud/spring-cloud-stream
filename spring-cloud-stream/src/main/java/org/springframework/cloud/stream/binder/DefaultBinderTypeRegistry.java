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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of a {@link BinderTypeRegistry}.
 *
 * @author Marius Bogoevici
 */
public class DefaultBinderTypeRegistry implements BinderTypeRegistry {

	private final Map<String, BinderType> binderTypes;

	public DefaultBinderTypeRegistry(Map<String, BinderType> binderTypes) {
		this.binderTypes = Collections.unmodifiableMap(new HashMap<>(binderTypes));
	}

	@Override
	public BinderType get(String name) {
		return this.binderTypes.get(name);
	}

	@Override
	public Map<String, BinderType> getAll() {
		return this.binderTypes;
	}

}
