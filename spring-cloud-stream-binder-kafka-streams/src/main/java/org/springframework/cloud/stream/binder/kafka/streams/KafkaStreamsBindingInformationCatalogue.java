/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ProducerFactory;

/**
 * A catalogue that provides binding information for Kafka Streams target types such as
 * KStream. It also keeps a catalogue for the underlying {@link StreamsBuilderFactoryBean}
 * and {@link StreamsConfig} associated with various
 * {@link org.springframework.cloud.stream.annotation.StreamListener} methods in the
 * {@link org.springframework.context.ApplicationContext}.
 *
 * @author Soby Chacko
 */
public class KafkaStreamsBindingInformationCatalogue {

	private final Map<KStream<?, ?>, BindingProperties> bindingProperties = new ConcurrentHashMap<>();

	private final Map<KStream<?, ?>, KafkaStreamsConsumerProperties> consumerProperties = new ConcurrentHashMap<>();

	private final Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans = new HashSet<>();

	private final Map<Object, ResolvableType> outboundKStreamResolvables = new HashMap<>();

	private final Map<KStream<?, ?>, Serde<?>> keySerdeInfo = new HashMap<>();

	private final Map<Object, String> bindingNamesPerTarget = new HashMap<>();

	private final List<ProducerFactory<byte[], byte[]>> dlqProducerFactories = new ArrayList<>();

	/**
	 * For a given bounded {@link KStream}, retrieve it's corresponding destination on the
	 * broker.
	 * @param bindingTarget binding target for KStream
	 * @return destination topic on Kafka
	 */
	String getDestination(KStream<?, ?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		return bindingProperties.getDestination();
	}

	/**
	 * Is native decoding is enabled on this {@link KStream}.
	 * @param bindingTarget binding target for KStream
	 * @return true if native decoding is enabled, fasle otherwise.
	 */
	boolean isUseNativeDecoding(KStream<?, ?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		if (bindingProperties.getConsumer() == null) {
			bindingProperties.setConsumer(new ConsumerProperties());
		}
		return bindingProperties.getConsumer().isUseNativeDecoding();
	}

	/**
	 * Is DLQ enabled for this {@link KStream}.
	 * @param bindingTarget binding target for KStream
	 * @return true if DLQ is enabled, false otherwise.
	 */
	boolean isDlqEnabled(KStream<?, ?> bindingTarget) {
		return this.consumerProperties.get(bindingTarget).isEnableDlq();
	}

	/**
	 * Retrieve the content type associated with a given {@link KStream}.
	 * @param bindingTarget binding target for KStream
	 * @return content Type associated.
	 */
	String getContentType(KStream<?, ?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		return bindingProperties.getContentType();
	}

	/**
	 * Register a cache for bounded KStream -> {@link BindingProperties}.
	 * @param bindingTarget binding target for KStream
	 * @param bindingProperties {@link BindingProperties} for this KStream
	 */
	void registerBindingProperties(KStream<?, ?> bindingTarget,
			BindingProperties bindingProperties) {
		if (bindingProperties != null) {
			this.bindingProperties.put(bindingTarget, bindingProperties);
		}
	}

	/**
	 * Register a cache for bounded KStream -> {@link KafkaStreamsConsumerProperties}.
	 * @param bindingTarget binding target for KStream
	 * @param kafkaStreamsConsumerProperties consumer properties for this KStream
	 */
	void registerConsumerProperties(KStream<?, ?> bindingTarget,
			KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties) {
		if (kafkaStreamsConsumerProperties != null) {
			this.consumerProperties.put(bindingTarget, kafkaStreamsConsumerProperties);
		}
	}

	/**
	 * Adds a mapping for KStream -> {@link StreamsBuilderFactoryBean}.
	 * @param streamsBuilderFactoryBean provides the {@link StreamsBuilderFactoryBean}
	 * mapped to the KStream
	 */
	void addStreamBuilderFactory(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBeans.add(streamsBuilderFactoryBean);
	}

	Set<StreamsBuilderFactoryBean> getStreamsBuilderFactoryBeans() {
		return this.streamsBuilderFactoryBeans;
	}

	void addOutboundKStreamResolvable(Object key, ResolvableType outboundResolvable) {
		this.outboundKStreamResolvables.put(key, outboundResolvable);
	}

	ResolvableType getOutboundKStreamResolvable(Object key) {
		return outboundKStreamResolvables.get(key);
	}

	/**
	 * Adding a mapping for KStream target to its corresponding KeySerde.
	 * This is used for sending to DLQ when deserialization fails. See {@link KafkaStreamsMessageConversionDelegate}
	 * for details.
	 *
	 * @param kStreamTarget target KStream
	 * @param keySerde Serde used for the key
	 */
	void addKeySerde(KStream<?, ?> kStreamTarget, Serde<?> keySerde) {
		this.keySerdeInfo.put(kStreamTarget, keySerde);
	}

	Serde<?> getKeySerde(KStream<?, ?> kStreamTarget) {
		return this.keySerdeInfo.get(kStreamTarget);
	}


	Map<KStream<?, ?>, BindingProperties> getBindingProperties() {
		return bindingProperties;
	}

	Map<KStream<?, ?>, KafkaStreamsConsumerProperties> getConsumerProperties() {
		return consumerProperties;
	}

	void addBindingNamePerTarget(Object target, String bindingName) {
		this.bindingNamesPerTarget.put(target, bindingName);
	}

	String bindingNamePerTarget(Object target) {
		return this.bindingNamesPerTarget.get(target);
	}

	public List<ProducerFactory<byte[], byte[]>> getDlqProducerFactories() {
		return this.dlqProducerFactories;
	}

	public void addDlqProducerFactory(ProducerFactory<byte[], byte[]> producerFactory) {
		this.dlqProducerFactories.add(producerFactory);
	}
}
