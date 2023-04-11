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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * Container for Pulsar specific extended producer and consumer binding properties.
 * <p>
 * These properties are applied to individual bindings and will override any binder-level
 * setting.
 *
 * <p>
 * <em>NOTE:</em> This class is only referenced as a value in the
 * {@link PulsarExtendedBindingProperties#getBindings() bindings map} and therefore, by
 * default is not included in the generated configuration metadata. To get around this
 * limitation it is annotated with {@code @ConfigurationProperties}. However, that is the
 * only reason it is annotated and is not intended to be used directly.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@SuppressWarnings("ConfigurationProperties")
@ConfigurationProperties("spring.cloud.stream.pulsar.bindings.for-docs-only")
public class PulsarBindingProperties implements BinderSpecificPropertiesProvider {

	/**
	 * Pulsar consumer specific binding properties.
	 */
	@NestedConfigurationProperty
	private PulsarConsumerProperties consumer = new PulsarConsumerProperties();

	/**
	 * Pulsar producer specific binding properties.
	 */
	@NestedConfigurationProperty
	private PulsarProducerProperties producer = new PulsarProducerProperties();

	public PulsarConsumerProperties getConsumer() {
		return this.consumer;
	}

	public void setConsumer(PulsarConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public PulsarProducerProperties getProducer() {
		return this.producer;
	}

	public void setProducer(PulsarProducerProperties producer) {
		this.producer = producer;
	}

}
