/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedBindingProperties;

/**
 * @author Marius Bogoevici
 */
@ConfigurationProperties("spring.cloud.stream.kafka.streams")
public class KafkaStreamsExtendedBindingProperties
		implements ExtendedBindingProperties<KafkaStreamsConsumerProperties, KafkaStreamsProducerProperties> {

	private Map<String, KafkaStreamsBindingProperties> bindings = new HashMap<>();

	public Map<String, KafkaStreamsBindingProperties> getBindings() {
		return this.bindings;
	}

	public void setBindings(Map<String, KafkaStreamsBindingProperties> bindings) {
		this.bindings = bindings;
	}

	@Override
	public KafkaStreamsConsumerProperties getExtendedConsumerProperties(String binding) {
		if (this.bindings.containsKey(binding) && this.bindings.get(binding).getConsumer() != null) {
			return this.bindings.get(binding).getConsumer();
		}
		else {
			return new KafkaStreamsConsumerProperties();
		}
	}

	@Override
	public KafkaStreamsProducerProperties getExtendedProducerProperties(String binding) {
		if (this.bindings.containsKey(binding) && this.bindings.get(binding).getProducer() != null) {
			return this.bindings.get(binding).getProducer();
		}
		else {
			return new KafkaStreamsProducerProperties();
		}
	}

	@Override
	public String getDefaultsPrefix() {
		return null;
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return KafkaStreamsBindingProperties.class;
	}
}
