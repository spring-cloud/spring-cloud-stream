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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

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
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KStreamExtendedBindingProperties kStreamExtendedBindingProperties;

	private final StreamsConfig streamsConfig;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	private Predicate[] predicates;

	private final MessageConversionDelegate messageConversionDelegate;

	public KStreamBinder(KafkaBinderConfigurationProperties binderConfigurationProperties,
						KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamExtendedBindingProperties kStreamExtendedBindingProperties, StreamsConfig streamsConfig,
						MessageConversionDelegate messageConversionDelegate) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
		this.streamsConfig = streamsConfig;
		this.messageConversionDelegate = messageConversionDelegate;
	}

	@Override
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
															KStream<Object, Object> inputTarget,
															ExtendedConsumerProperties<KStreamConsumerProperties> properties) {

		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				new KafkaConsumerProperties());
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);
		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name, KStream<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KStreamProducerProperties> properties) {
		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		this.kafkaTopicProvisioner.provisionProducerDestination(name, extendedProducerProperties);

		String[] branches = new String[]{};
		if (predicates != null && predicates.length > 0) {
			String additionalBranches = properties.getExtension().getAdditionalBranches();
			if (!StringUtils.hasText(additionalBranches)) {
				Assert.isTrue(predicates.length == 1, "More than 1 predicate bean found, but no additional output branches");
			}
			else {
				branches = StringUtils.commaDelimitedListToStringArray(additionalBranches);
				Assert.isTrue(branches.length + 1 >= predicates.length,
						"Number of output topics and org.apache.kafka.streams.kstream.Predicate[] beans don't match");
				for (String branch : branches) {
					this.kafkaTopicProvisioner.provisionProducerDestination(branch, extendedProducerProperties);
				}
			}
		}

		Serde<?> keySerde = getKeySerde(properties);
		Serde<?> valueSerde = getValueSerde(properties);

		to(properties.isUseNativeEncoding(), name, outboundBindTarget, (Serde<Object>) keySerde, (Serde<Object>) valueSerde, branches);

		return new DefaultBinding<>(name, null, outboundBindTarget, null);
	}

	private Serde<?> getKeySerde(ExtendedProducerProperties<KStreamProducerProperties> properties) {
		Serde<?> keySerde;
		try {
			if (properties.isUseNativeEncoding()) {
				keySerde = this.binderConfigurationProperties.getConfiguration().containsKey("key.serde") ?
						Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("key.serde"), Serde.class) : Serdes.ByteArray();

			}
			else {
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
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return keySerde;
	}

	private Serde<?> getValueSerde(ExtendedProducerProperties<KStreamProducerProperties> properties) {
		Serde<?> valueSerde;
		try {
			if (properties.isUseNativeEncoding()) {
				valueSerde = this.binderConfigurationProperties.getConfiguration().containsKey("value.serde") ?
						Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("value.serde"), Serde.class) : Serdes.ByteArray();

			}
			else {

				if (StringUtils.hasText(properties.getExtension().getValueSerde())) {
					valueSerde = Utils.newInstance(properties.getExtension().getValueSerde(), Serde.class);
					if (valueSerde instanceof Configurable) {
						((Configurable) valueSerde).configure(streamsConfig.originals());
					}
				}
				else {
					valueSerde = Serdes.ByteArray();
				}
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return valueSerde;
	}

	@SuppressWarnings("unchecked")
	private void to(boolean isNativeEncoding, String name, KStream<Object, Object> outboundBindTarget,
				Serde<Object> keySerde, Serde<Object> valueSerde, String[] branches) {
		KeyValueMapper<Object, Object, KeyValue<Object, Object>> keyValueMapper = null;
		if (!isNativeEncoding) {
			keyValueMapper = messageConversionDelegate.outboundKeyValueMapper(name);
		}
		if (predicates != null && predicates.length > 0) {
			KStream[] toBranches = outboundBindTarget.branch(predicates);
			String[] topics = getOutputTopicsInProperOrder(name, branches);
			for (int i = 0; i < toBranches.length; i++) {
				if (!isNativeEncoding) {
					toBranches[i].map(keyValueMapper).to(topics[i], Produced.with(keySerde, valueSerde));
				}
				else {
					toBranches[i].to(topics[i], Produced.with(keySerde, valueSerde));
				}
			}
		} else {
			if (!isNativeEncoding) {
				outboundBindTarget.map(keyValueMapper).to(name, Produced.with(keySerde, valueSerde));
			}
			else {
				outboundBindTarget.to(name, Produced.with(keySerde, valueSerde));
			}
		}
	}

	private static String[] getOutputTopicsInProperOrder(String name, String[] branches) {
		String[] topics = new String[branches.length + 1];
		topics[0] = name;
		int j = 1;
		for (String branch : branches) {
			topics[j++] = branch;
		}
		return topics;
	}

	@Override
	public KStreamConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KStreamProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setPredicates(Predicate[] predicates) {
		this.predicates = predicates;
	}

}
