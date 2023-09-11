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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.pulsar.properties.ProducerConfigProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Unit tests for {@link ProducerConfigProperties}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
class ProducerConfigPropertiesTests {

	@Test
	void basePropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		var producerConfigProps = bindInputPropsToProducerConfigProps(inputProps);
		var outputProps = producerConfigProps.toBaseProducerPropertiesMap();
		verifyOutputPropsCanBeLoadedInProducerBuilder(outputProps);
		verifyBasePropsInOutputMap(outputProps);
	}

	private Map<String, String> basePropsInputMap() {
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.pulsar.producer.access-mode", "Exclusive");
		inputProps.put("spring.pulsar.producer.batching-enabled", "true");
		inputProps.put("spring.pulsar.producer.chunking-enabled", "true");
		inputProps.put("spring.pulsar.producer.compression-type", "lz4");
		inputProps.put("spring.pulsar.producer.hashing-scheme", "murmur3_32hash");
		inputProps.put("spring.pulsar.producer.message-routing-mode", "custompartition");
		inputProps.put("spring.pulsar.producer.name", "my-producer");
		inputProps.put("spring.pulsar.producer.send-timeout", "2s");
		inputProps.put("spring.pulsar.producer.topic-name", "my-topic");
		return inputProps;
	}

	private void verifyBasePropsInOutputMap(Map<String, Object> outputProps) {
		assertThat(outputProps).containsEntry("accessMode", ProducerAccessMode.Exclusive)
				.containsEntry("batchingEnabled", true).containsEntry("chunkingEnabled", true)
				.containsEntry("compressionType", CompressionType.LZ4)
				.containsEntry("hashingScheme", HashingScheme.Murmur3_32Hash)
				.containsEntry("messageRoutingMode", MessageRoutingMode.CustomPartition)
				.containsEntry("producerName", "my-producer").containsEntry("sendTimeoutMs", 2_000)
				.containsEntry("topicName", "my-topic");
	}

	@Test
	void extendedPropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		inputProps.putAll(extendedPropsInputMap());
		var producerConfigProps = bindInputPropsToProducerConfigProps(inputProps);
		var outputProps = producerConfigProps.toExtendedProducerPropertiesMap();
		verifyOutputPropsCanBeLoadedInProducerBuilder(outputProps);
		verifyExtendedPropsInOutputMap(outputProps);
	}

	@Test
	void allPropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		inputProps.putAll(extendedPropsInputMap());
		var producerConfigProps = bindInputPropsToProducerConfigProps(inputProps);
		var outputProps = producerConfigProps.toAllProducerPropertiesMap();
		verifyOutputPropsCanBeLoadedInProducerBuilder(outputProps);
		verifyBasePropsInOutputMap(outputProps);
		verifyExtendedPropsInOutputMap(outputProps);
	}

	@Test
	void batchPropsSkippedWhenBatchDisabled() {
		var inputProps = basePropsInputMap();
		inputProps.putAll(extendedPropsInputMap());
		inputProps.put("spring.pulsar.producer.batching-enabled", "false");
		var producerConfigProps = bindInputPropsToProducerConfigProps(inputProps);
		var outputProps = producerConfigProps.toAllProducerPropertiesMap();
		assertThat(outputProps).doesNotContainKey("batchingMaxPublishDelayMicros")
				.doesNotContainKey("batchingPartitionSwitchFrequencyByPublishDelay")
				.doesNotContainKey("batchingMaxMessages").doesNotContainKey("batchingMaxBytes");
	}

	private Map<String, String> extendedPropsInputMap() {
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.pulsar.producer.auto-update-partitions", "true");
		inputProps.put("spring.pulsar.producer.auto-update-partitions-interval", "15s");
		inputProps.put("spring.pulsar.producer.block-if-queue-full", "true");
		inputProps.put("spring.pulsar.producer.crypto-failure-action", "send");
		inputProps.put("spring.pulsar.producer.encryption-keys[0]", "my-key");
		inputProps.put("spring.pulsar.producer.initial-sequence-id", "9");
		inputProps.put("spring.pulsar.producer.lazy-start=partitioned-producers", "true");
		inputProps.put("spring.pulsar.producer.max-pending-messages", "3");
		inputProps.put("spring.pulsar.producer.max-pending-messages-across-partitions", "4");
		inputProps.put("spring.pulsar.producer.multi-schema", "true");
		inputProps.put("spring.pulsar.producer.properties[my-prop]", "my-prop-value");
		inputProps.put("spring.pulsar.producer.batch.max-publish-delay", "5s");
		inputProps.put("spring.pulsar.producer.batch.partition-switch-frequency-by-publish-delay", "6");
		inputProps.put("spring.pulsar.producer.batch.max-messages", "7");
		inputProps.put("spring.pulsar.producer.batch.max-bytes", "8");
		return inputProps;
	}

	private void verifyExtendedPropsInOutputMap(Map<String, Object> outputProps) {
		assertThat(outputProps).containsEntry("autoUpdatePartitions", true)
				.containsEntry("autoUpdatePartitionsIntervalSeconds", 15L).containsEntry("blockIfQueueFull", true)
				.containsEntry("cryptoFailureAction", ProducerCryptoFailureAction.SEND)
				.hasEntrySatisfying("encryptionKeys",
						keys -> assertThat(keys).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
								.containsExactly("my-key"))
				.containsEntry("initialSequenceId", 9L).containsEntry("lazyStartPartitionedProducers", true)
				.containsEntry("maxPendingMessages", 3).containsEntry("maxPendingMessagesAcrossPartitions", 4)
				.containsEntry("multiSchema", true)
				.hasEntrySatisfying("properties",
						properties -> assertThat(properties)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
								.containsEntry("my-prop", "my-prop-value"))
				.containsEntry("batchingMaxPublishDelayMicros", 5_000_000L)
				.containsEntry("batchingPartitionSwitchFrequencyByPublishDelay", 6)
				.containsEntry("batchingMaxMessages", 7).containsEntry("batchingMaxBytes", 8);
	}

	private void verifyOutputPropsCanBeLoadedInProducerBuilder(Map<String, Object> outputProps) {
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(outputProps,
				new ProducerConfigurationData(), ProducerConfigurationData.class));
	}

	private ProducerConfigProperties bindInputPropsToProducerConfigProps(Map<String, String> inputProps) {
		return new Binder(new MapConfigurationPropertySource(inputProps))
				.bind("spring.pulsar.producer", Bindable.ofInstance(new ProducerConfigProperties())).get();
	}

}
