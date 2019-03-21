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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.util.StringUtils;

/**
 * Resolver for key and value Serde.
 *
 * On the inbound, if native decoding is enabled, then any deserialization on the value is
 * handled by Kafka. First, we look for any key/value Serde set on the binding itself, if
 * that is not available then look at the common Serde set at the global level. If that
 * fails, it falls back to byte[]. If native decoding is disabled, then the binder will do
 * the deserialization on value and ignore any Serde set for value and rely on the
 * contentType provided. Keys are always deserialized at the broker.
 *
 *
 * Same rules apply on the outbound. If native encoding is enabled, then value
 * serialization is done at the broker using any binder level Serde for value, if not
 * using common Serde, if not, then byte[]. If native encoding is disabled, then the
 * binder will do serialization using a contentType. Keys are always serialized by the
 * broker.
 *
 * For state store, use serdes class specified in
 * {@link org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore}
 * to create Serde accordingly.
 *
 * @author Soby Chacko
 * @author Lei Chen
 */
class KeyValueSerdeResolver {

	private final Map<String, Object> streamConfigGlobalProperties;

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	KeyValueSerdeResolver(Map<String, Object> streamConfigGlobalProperties,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		this.streamConfigGlobalProperties = streamConfigGlobalProperties;
		this.binderConfigurationProperties = binderConfigurationProperties;
	}

	/**
	 * Provide the {@link Serde} for inbound key.
	 * @param extendedConsumerProperties binding level extended
	 * {@link KafkaStreamsConsumerProperties}
	 * @return configurd {@link Serde} for the inbound key.
	 */
	public Serde<?> getInboundKeySerde(
			KafkaStreamsConsumerProperties extendedConsumerProperties) {
		String keySerdeString = extendedConsumerProperties.getKeySerde();

		return getKeySerde(keySerdeString);
	}

	/**
	 * Provide the {@link Serde} for inbound value.
	 * @param consumerProperties {@link ConsumerProperties} on binding
	 * @param extendedConsumerProperties binding level extended
	 * {@link KafkaStreamsConsumerProperties}
	 * @return configurd {@link Serde} for the inbound value.
	 */
	public Serde<?> getInboundValueSerde(ConsumerProperties consumerProperties,
			KafkaStreamsConsumerProperties extendedConsumerProperties) {
		Serde<?> valueSerde;

		String valueSerdeString = extendedConsumerProperties.getValueSerde();
		try {
			if (consumerProperties != null && consumerProperties.isUseNativeDecoding()) {
				valueSerde = getValueSerde(valueSerdeString);
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
			valueSerde.configure(this.streamConfigGlobalProperties, false);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	/**
	 * Provide the {@link Serde} for outbound key.
	 * @param properties binding level extended {@link KafkaStreamsProducerProperties}
	 * @return configurd {@link Serde} for the outbound key.
	 */
	public Serde<?> getOuboundKeySerde(KafkaStreamsProducerProperties properties) {
		return getKeySerde(properties.getKeySerde());
	}

	/**
	 * Provide the {@link Serde} for outbound value.
	 * @param producerProperties {@link ProducerProperties} on binding
	 * @param kafkaStreamsProducerProperties binding level extended
	 * {@link KafkaStreamsProducerProperties}
	 * @return configurd {@link Serde} for the outbound value.
	 */
	public Serde<?> getOutboundValueSerde(ProducerProperties producerProperties,
			KafkaStreamsProducerProperties kafkaStreamsProducerProperties) {
		Serde<?> valueSerde;
		try {
			if (producerProperties.isUseNativeEncoding()) {
				valueSerde = getValueSerde(
						kafkaStreamsProducerProperties.getValueSerde());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
			valueSerde.configure(this.streamConfigGlobalProperties, false);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	/**
	 * Provide the {@link Serde} for state store.
	 * @param keySerdeString serde class used for key
	 * @return {@link Serde} for the state store key.
	 */
	public Serde<?> getStateStoreKeySerde(String keySerdeString) {
		return getKeySerde(keySerdeString);
	}

	/**
	 * Provide the {@link Serde} for state store value.
	 * @param valueSerdeString serde class used for value
	 * @return {@link Serde} for the state store value.
	 */
	public Serde<?> getStateStoreValueSerde(String valueSerdeString) {
		try {
			return getValueSerde(valueSerdeString);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
	}

	private Serde<?> getKeySerde(String keySerdeString) {
		Serde<?> keySerde;
		try {
			if (StringUtils.hasText(keySerdeString)) {
				keySerde = Utils.newInstance(keySerdeString, Serde.class);
			}
			else {
				keySerde = this.binderConfigurationProperties.getConfiguration()
						.containsKey("default.key.serde")
								? Utils.newInstance(this.binderConfigurationProperties
										.getConfiguration().get("default.key.serde"),
										Serde.class)
								: Serdes.ByteArray();
			}
			keySerde.configure(this.streamConfigGlobalProperties, true);

		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return keySerde;
	}

	private Serde<?> getValueSerde(String valueSerdeString)
			throws ClassNotFoundException {
		Serde<?> valueSerde;
		if (StringUtils.hasText(valueSerdeString)) {
			valueSerde = Utils.newInstance(valueSerdeString, Serde.class);
		}
		else {
			valueSerde = this.binderConfigurationProperties.getConfiguration()
					.containsKey("default.value.serde")
							? Utils.newInstance(this.binderConfigurationProperties
									.getConfiguration().get("default.value.serde"),
									Serde.class)
							: Serdes.ByteArray();
		}
		return valueSerde;
	}

}
