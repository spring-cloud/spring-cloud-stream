/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;

/**
 * A {@link Map} implementation that initializes its entries by binding values from the
 * supplied environment. Any call to 'get()' will result in either returning the existing
 * value or initializing a new entry by binding properties with the specified prefix from
 * the environment.
 *
 * This is strictly intended to be used for configuration property values and not to be
 * used as a general purpose map.
 *
 * This implementation is not thread safe.
 *
 * @author Marius Bogoevici
 * @author Janne Valkealahti
 */
public class EnvironmentEntryInitializingTreeMap<T> extends AbstractMap<String, T> {

	private final ConfigurableEnvironment environment;

	private final Class<T> entryClass;

	private final String defaultsPrefix;

	private final Map<String, T> delegate;

	/**
	 * Constructs the map.
	 *
	 * @param environment the environment that supplies the default property values
	 * @param entryClass the entry class
	 * @param defaultsPrefix the prefix for initializing the properties
	 * @param delegate the actual map that stores the values
	 */
	public EnvironmentEntryInitializingTreeMap(ConfigurableEnvironment environment, Class<T> entryClass,
			String defaultsPrefix, Map<String, T> delegate) {
		Assert.notNull(environment, "The environment cannot be null");
		Assert.notNull(entryClass, "The entry class cannot be null");
		Assert.notNull(defaultsPrefix, "The prefix for the property defaults cannot be null");
		Assert.notNull(delegate, "The delegate cannot be null");
		this.environment = environment;
		this.entryClass = entryClass;
		this.defaultsPrefix = defaultsPrefix;
		this.delegate = delegate;
	}

	@Override
	public T get(Object key) {
		if (!this.delegate.containsKey(key) && key instanceof String) {
			T entry = BeanUtils.instantiate(entryClass);
			Binder.get(environment).bind(defaultsPrefix, Bindable.ofInstance(entry));
			this.delegate.put((String) key, entry);
		}
		return this.delegate.get(key);
	}

	@Override
	public T put(String key, T value) {
		// boot 2 call this first
		Binder.get(environment).bind(defaultsPrefix, Bindable.ofInstance(value));
		return this.delegate.put(key, value);
	}

	@Override
	public Set<Entry<String, T>> entrySet() {
		return delegate.entrySet();
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}
}
