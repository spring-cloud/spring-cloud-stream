/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 */
@ConfigurationProperties("spring.cloud.stream.rabbit")
public class RabbitExtendedBindingProperties extends
		AbstractExtendedBindingProperties<RabbitConsumerProperties, RabbitProducerProperties, RabbitBindingProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.rabbit.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	public Map<String, RabbitBindingProperties> getBindings() {
		return this.doGetBindings();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return RabbitBindingProperties.class;
	}

}
