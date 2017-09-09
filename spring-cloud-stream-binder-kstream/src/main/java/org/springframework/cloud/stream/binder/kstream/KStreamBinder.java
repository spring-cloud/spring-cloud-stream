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

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private String[] headers;

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KStreamExtendedBindingProperties kStreamExtendedBindingProperties;

	private final StreamsConfig streamsConfig;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	public KStreamBinder(KafkaBinderConfigurationProperties binderConfigurationProperties, KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamExtendedBindingProperties kStreamExtendedBindingProperties, StreamsConfig streamsConfig) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.headers = EmbeddedHeaderUtils.headersToEmbed(binderConfigurationProperties.getHeaders());
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
		if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {
			outboundBindTarget = outboundBindTarget.map(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
				@Override
				public KeyValue<Object, Object> apply(Object k, Object v) {
					if (v instanceof Message) {
						try {
							return new KeyValue<>(k, (Object) KStreamBinder.this.serializeAndEmbedHeadersIfApplicable((Message<?>) v));
						}
						catch (Exception e) {
							throw new IllegalArgumentException(e);
						}
					}
					else {
						throw new IllegalArgumentException("Wrong type of message " + v);
					}
				}
			});
		}
		else {
			if (!properties.isUseNativeEncoding()) {
				outboundBindTarget = outboundBindTarget
						.map(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
							@Override
							public KeyValue<Object, Object> apply(Object k, Object v) {
								return KeyValue.pair(k, (Object) KStreamBinder.this.serializePayloadIfNecessary((Message<?>) v));
							}
						});
			}
			else {
				outboundBindTarget = outboundBindTarget
						.map(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
							@Override
							public KeyValue<Object, Object> apply(Object k, Object v) {
								return KeyValue.pair(k, ((Message<?>) v).getPayload());
							}
						});
			}
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

	private byte[] serializeAndEmbedHeadersIfApplicable(Message<?> message) throws Exception {
		MessageValues transformed = serializePayloadIfNecessary(message);
		byte[] payload;

		Object contentType = transformed.get(MessageHeaders.CONTENT_TYPE);
		// transform content type headers to String, so that they can be properly embedded
		// in JSON
		if (contentType instanceof MimeType) {
			transformed.put(MessageHeaders.CONTENT_TYPE, contentType.toString());
		}
		Object originalContentType = transformed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
		if (originalContentType instanceof MimeType) {
			transformed.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		payload = EmbeddedHeaderUtils.embedHeaders(transformed, headers);
		return payload;
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
