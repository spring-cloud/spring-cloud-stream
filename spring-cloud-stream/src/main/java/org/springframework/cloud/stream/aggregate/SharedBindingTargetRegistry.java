/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Stores binding targets shared by the components of an aggregate application.
 *
 * @author Marius Bogoevici
 * @since 1.1.1
 */
public class SharedBindingTargetRegistry {

	private Map<String, Object> sharedBindingTargets = new ConcurrentSkipListMap<>(
			String.CASE_INSENSITIVE_ORDER);

	@SuppressWarnings("unchecked")
	public <T> T get(String id, Class<T> bindingTargetType) {
		Object sharedBindingTarget = this.sharedBindingTargets.get(id);
		if (sharedBindingTarget == null) {
			return null;
		}
		if (!bindingTargetType.isAssignableFrom(sharedBindingTarget.getClass())) {
			throw new IllegalArgumentException("A shared " + bindingTargetType.getName()
					+ " was requested, " + "but the existing shared target with id '" + id
					+ "' is a " + sharedBindingTarget.getClass());
		}
		else {
			return (T) sharedBindingTarget;
		}
	}

	public void register(String id, Object bindingTarget) {
		this.sharedBindingTargets.put(id, bindingTarget);
	}

	public Map<String, Object> getAll() {
		return Collections.unmodifiableMap(this.sharedBindingTargets);
	}

}
