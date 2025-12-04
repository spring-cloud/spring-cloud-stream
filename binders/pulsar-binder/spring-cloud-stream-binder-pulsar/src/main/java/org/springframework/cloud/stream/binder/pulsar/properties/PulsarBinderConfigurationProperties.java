/*
 * Copyright 2023-present the original author or authors.
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
import org.springframework.lang.Nullable;

/**
 * {@link ConfigurationProperties @ConfigurationProperties} for the Pulsar binder.
 * <p>
 * These properties are applied at the binder level (to all bindings).
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.pulsar.binder")
public class PulsarBinderConfigurationProperties {

	/**
	 * Pulsar consumer specific binder-level properties (applied to all bindings).
	 */
	@NestedConfigurationProperty
	private final ConsumerConfigProperties consumer = new ConsumerConfigProperties();

	/**
	 * Pulsar producer specific binder-level properties (applied to all bindings).
	 */
	@NestedConfigurationProperty
	private final ProducerConfigProperties producer = new ProducerConfigProperties();

	/**
	 * Number of topic partitions.
	 */
	@Nullable
	private Integer partitionCount;

	public ConsumerConfigProperties getConsumer() {
		return this.consumer;
	}

	public ProducerConfigProperties getProducer() {
		return this.producer;
	}

	@Nullable
	public Integer getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(Integer partitionCount) {
		this.partitionCount = partitionCount;
	}

}
