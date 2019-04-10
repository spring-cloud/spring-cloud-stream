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

package org.springframework.cloud.stream.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor;
import org.springframework.boot.context.properties.bind.AbstractBindHandler;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName.Form;
import org.springframework.util.CollectionUtils;

/**
 * @author Oleg Zhurakousky
 * @since 2.1
 *
 */
public class BindingHandlerAdvise implements ConfigurationPropertiesBindHandlerAdvisor {

	private final Map<ConfigurationPropertyName, ConfigurationPropertyName> mappings;


	BindingHandlerAdvise(
			Map<ConfigurationPropertyName, ConfigurationPropertyName> additionalMappings) {
		this.mappings = new LinkedHashMap<>();
		this.mappings.put(ConfigurationPropertyName.of("spring.cloud.stream.bindings"),
				ConfigurationPropertyName.of("spring.cloud.stream.default"));
		if (!CollectionUtils.isEmpty(additionalMappings)) {
			this.mappings.putAll(additionalMappings);
		}
	}

	@Override
	public BindHandler apply(BindHandler bindHandler) {


		BindHandler handler = new AbstractBindHandler(bindHandler) {
			@Override
			public <T> Bindable<T> onStart(ConfigurationPropertyName name,
					Bindable<T> target, BindContext context) {
				ConfigurationPropertyName defaultName = getDefaultName(name);
				if (defaultName != null) {
					BindResult<T> result = context.getBinder().bind(defaultName, target);
					if (result.isBound()) {
						return target.withExistingValue(result.get());
					}
				}
				return bindHandler.onStart(name, target, context);
			}
		};
		return handler;
	}

	private ConfigurationPropertyName getDefaultName(ConfigurationPropertyName name) {
		for (Map.Entry<ConfigurationPropertyName, ConfigurationPropertyName> mapping : this.mappings
				.entrySet()) {
			ConfigurationPropertyName from = mapping.getKey();
			ConfigurationPropertyName to = mapping.getValue();
			if ((from.isAncestorOf(name)
					&& name.getNumberOfElements() > from.getNumberOfElements())) {
				ConfigurationPropertyName defaultName = to;
				for (int i = from.getNumberOfElements() + 1; i < name
						.getNumberOfElements(); i++) {
					defaultName = defaultName.append(name.getElement(i, Form.UNIFORM));
				}
				return defaultName;
			}
		}
		return null;
	}

	/**
	 * Provides mappings including the default mappings.
	 */
	public interface MappingsProvider {

		Map<ConfigurationPropertyName, ConfigurationPropertyName> getDefaultMappings();

	}

}
