/*
 * Copyright 2018-2019 the original author or authors.
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

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.support.utils.IntegrationUtils;

/**
 * Base implementation of {@link ExtendedBindingProperties}.
 *
 * @param <C> - consumer properties type
 * @param <P> - producer properties type
 * @param <T> - type which provides the consumer and producer properties
 * @author Oleg Zhurakousky
 * @since 2.1
 */
public abstract class AbstractExtendedBindingProperties<C, P, T extends BinderSpecificPropertiesProvider>
		implements ExtendedBindingProperties<C, P>, ApplicationContextAware {

	private final Map<String, T> bindings = new HashMap<>();

	private ConfigurableApplicationContext applicationContext = new GenericApplicationContext();

	public void setBindings(Map<String, T> bindings) {
		this.bindings.putAll(bindings);
	}

	@SuppressWarnings("unchecked")
	@Override
	public C getExtendedConsumerProperties(String binding) {
		this.bindIfNecessary(binding);
		return (C) this.bindings.get(binding).getConsumer();
	}

	@SuppressWarnings("unchecked")
	@Override
	public P getExtendedProducerProperties(String binding) {
		this.bindIfNecessary(binding);
		return (P) this.bindings.get(binding).getProducer();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	/*
	 * The "necessary" implies the scenario where only defaults are defined.
	 */
	private void bindIfNecessary(String bindingName) {
		if (!this.bindings.containsKey(bindingName)) {
			this.bindToDefault(bindingName);
		}
	}

	@SuppressWarnings("unchecked")
	private void bindToDefault(String binding) {
		T extendedBindingPropertiesTarget = (T) BeanUtils
				.instantiateClass(this.getExtendedPropertiesEntryClass());
		Binder binder = new Binder(
				ConfigurationPropertySources
						.get(this.applicationContext.getEnvironment()),
				new PropertySourcesPlaceholdersResolver(
						this.applicationContext.getEnvironment()),
				IntegrationUtils.getConversionService(
						this.applicationContext.getBeanFactory()),
				null);
		binder.bind(this.getDefaultsPrefix(),
				Bindable.ofInstance(extendedBindingPropertiesTarget));
		this.bindings.put(binding, extendedBindingPropertiesTarget);
	}

	protected Map<String, T> doGetBindings() {
		return Collections.unmodifiableMap(this.bindings);
	}

}
