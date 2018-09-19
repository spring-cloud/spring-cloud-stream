/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedBindingProperties;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 */
@ConfigurationProperties("spring.cloud.stream.rabbit")
public class RabbitExtendedBindingProperties implements ExtendedBindingProperties<RabbitConsumerProperties, RabbitProducerProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.rabbit.default";

	private Map<String, RabbitBindingProperties> bindings = new HashMap<>();

	public Map<String, RabbitBindingProperties> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, RabbitBindingProperties> bindings) {
		this.bindings = bindings;
	}

	@Override
	public synchronized RabbitConsumerProperties getExtendedConsumerProperties(String channelName) {
		RabbitConsumerProperties properties;
		if (bindings.containsKey(channelName)) {
			if (bindings.get(channelName).getConsumer() != null) {
				properties = bindings.get(channelName).getConsumer();
			}
			else {
				properties = new RabbitConsumerProperties();
				this.bindings.get(channelName).setConsumer(properties);
			}
		}
		else {
			properties = new RabbitConsumerProperties();
			RabbitBindingProperties rbp = new RabbitBindingProperties();
			rbp.setConsumer(properties);
			bindings.put(channelName, rbp);
		}
		return properties;
	}

	@Override
	public synchronized RabbitProducerProperties getExtendedProducerProperties(String channelName) {
		RabbitProducerProperties properties;
		if (bindings.containsKey(channelName)) {
			if (bindings.get(channelName).getProducer() != null) {
				properties = bindings.get(channelName).getProducer();
			}
			else {
				properties = new RabbitProducerProperties();
				this.bindings.get(channelName).setProducer(properties);
			}
		}
		else {
			properties = new RabbitProducerProperties();
			RabbitBindingProperties rbp = new RabbitBindingProperties();
			rbp.setProducer(properties);
			bindings.put(channelName, rbp);
		}
		return properties;
	}

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return RabbitBindingProperties.class;
	}

}
