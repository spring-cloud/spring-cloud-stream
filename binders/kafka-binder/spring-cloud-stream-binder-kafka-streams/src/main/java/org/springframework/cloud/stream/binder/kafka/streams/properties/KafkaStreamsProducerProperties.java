/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;

/**
 * Extended properties for Kafka Streams producer.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KafkaStreamsProducerProperties extends KafkaProducerProperties {

	/**
	 * Key serde specified per binding.
	 */
	private String keySerde;

	/**
	 * Value serde specified per binding.
	 */
	private String valueSerde;

	/**
	 * {@link org.apache.kafka.streams.processor.StreamPartitioner} to be used on Kafka Streams producer.
	 */
	private String streamPartitionerBeanName;

	/**
	 * Custom name for the sink component to which the processor is producing to.
	 */
	private String producedAs;

	public String getKeySerde() {
		return this.keySerde;
	}

	public void setKeySerde(String keySerde) {
		this.keySerde = keySerde;
	}

	public String getValueSerde() {
		return this.valueSerde;
	}

	public void setValueSerde(String valueSerde) {
		this.valueSerde = valueSerde;
	}

	public String getStreamPartitionerBeanName() {
		return this.streamPartitionerBeanName;
	}

	public void setStreamPartitionerBeanName(String streamPartitionerBeanName) {
		this.streamPartitionerBeanName = streamPartitionerBeanName;
	}

	public String getProducedAs() {
		return producedAs;
	}

	public void setProducedAs(String producedAs) {
		this.producedAs = producedAs;
	}
}
