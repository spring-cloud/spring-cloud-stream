/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar.properties;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * {@link ConfigurationProperties @ConfigurationProperties} for Pulsar binder specific
 * extensions to the common binding properties.
 * <p>
 * These properties are applied to individual bindings and will override any binder-level
 * settings.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@ConfigurationProperties("spring.cloud.stream.pulsar")
public class PulsarExtendedBindingProperties extends
		AbstractExtendedBindingProperties<PulsarConsumerProperties, PulsarProducerProperties, PulsarBindingProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.pulsar.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	/**
	 * Properties per individual binding name (e.g. 'mySink-in-0').
	 */
	@Override
	public Map<String, PulsarBindingProperties> getBindings() {
		return this.doGetBindings();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return PulsarBindingProperties.class;
	}

}
