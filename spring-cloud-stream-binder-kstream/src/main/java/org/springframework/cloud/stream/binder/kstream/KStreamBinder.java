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

package org.springframework.cloud.stream.binder.kstream;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KStreamExtendedBindingProperties kStreamExtendedBindingProperties;

	private final StreamsConfig streamsConfig;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	public KStreamBinder(KafkaBinderConfigurationProperties binderConfigurationProperties, KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamExtendedBindingProperties kStreamExtendedBindingProperties, StreamsConfig streamsConfig) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
		this.streamsConfig = streamsConfig;
	}

	@Override
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
															KStream<Object, Object> inputTarget, ExtendedConsumerProperties<KStreamConsumerProperties> properties) {

		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<KafkaConsumerProperties>(
				new KafkaConsumerProperties());
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);
		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name, KStream<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KStreamProducerProperties> properties) {
		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<KafkaProducerProperties>(
				new KafkaProducerProperties());
		this.kafkaTopicProvisioner.provisionProducerDestination(name, extendedProducerProperties);
		if (!properties.isUseNativeEncoding()) {
			outboundBindTarget = outboundBindTarget
					.map((k, v) -> KeyValue.pair(k, (Object) KStreamBinder.this.serializePayloadIfNecessary((Message<?>) v)));
		}
		else {
			outboundBindTarget = outboundBindTarget
					.map((k, v) -> KeyValue.pair(k, ((Message<Object>) v).getPayload()));
		}
		if (!properties.isUseNativeEncoding() || StringUtils.hasText(properties.getExtension().getKeySerde()) || StringUtils.hasText(properties.getExtension().getValueSerde())) {
			try {
				Serde<?> keySerde;
				Serde<?> valueSerde;

				if (StringUtils.hasText(properties.getExtension().getKeySerde())) {
					keySerde = Utils.newInstance(properties.getExtension().getKeySerde(), Serde.class);
					if (keySerde instanceof Configurable) {
						((Configurable) keySerde).configure(streamsConfig.originals());
					}
				}
				else {
					keySerde = this.binderConfigurationProperties.getConfiguration().containsKey("key.serde") ?
							Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("key.serde"), Serde.class) : Serdes.ByteArray();
				}

				if (StringUtils.hasText(properties.getExtension().getValueSerde())) {
					valueSerde = Utils.newInstance(properties.getExtension().getValueSerde(), Serde.class);
					if (valueSerde instanceof Configurable) {
						((Configurable) valueSerde).configure(streamsConfig.originals());
					}
				}
				else {
					valueSerde = this.binderConfigurationProperties.getConfiguration().containsKey("value.serde") ?
							Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("value.serde"), Serde.class) : Serdes.ByteArray();
				}
				outboundBindTarget.to((Serde<Object>) keySerde, (Serde<Object>) valueSerde, name);
			}
			catch (ClassNotFoundException e) {
				throw new IllegalStateException("Serde class not found: ", e);
			}

		}
		else {
			outboundBindTarget.to(name);
		}
		return new DefaultBinding<>(name, null, outboundBindTarget, null);
	}

	@Override
	public KStreamConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KStreamProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}

}
