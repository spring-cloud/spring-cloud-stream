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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * Kafka specific extended binding properties class that extends from
 * {@link AbstractExtendedBindingProperties}.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 */
@ConfigurationProperties("spring.cloud.stream.kafka")
public class KafkaExtendedBindingProperties extends
		AbstractExtendedBindingProperties<KafkaConsumerProperties, KafkaProducerProperties, KafkaBindingProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.kafka.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	@Override
	public Map<String, KafkaBindingProperties> getBindings() {
		return this.doGetBindings();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return KafkaBindingProperties.class;
	}

}
