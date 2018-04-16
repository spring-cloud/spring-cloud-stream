/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

/**
 * @author Soby Chacko
 * @author Rafal Zukowski
 * @author Gary Russell
 */
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.ObjectUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

class KafkaStreamsDlqDispatch {

	private final Log logger = LogFactory.getLog(getClass());

	private final KafkaTemplate<byte[],byte[]> kafkaTemplate;

	private final String dlqName;

	KafkaStreamsDlqDispatch(String dlqName,
							KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
							KafkaConsumerProperties kafkaConsumerProperties) {
		ProducerFactory<byte[],byte[]> producerFactory = getProducerFactory(
				new ExtendedProducerProperties<>(kafkaConsumerProperties.getDlqProducerProperties()),
				kafkaBinderConfigurationProperties);

		this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
		this.dlqName = dlqName;
	}

	@SuppressWarnings("unchecked")
	public void sendToDlq(byte[] key, byte[] value, int partittion) {
		ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(this.dlqName, partittion,
				key, value, null);

		StringBuilder sb = new StringBuilder().append(" a message with key='")
				.append(toDisplayString(ObjectUtils.nullSafeToString(key))).append("'")
				.append(" and payload='")
				.append(toDisplayString(ObjectUtils.nullSafeToString(value)))
				.append("'").append(" received from ")
				.append(partittion);
		ListenableFuture<SendResult<byte[],byte[]>> sentDlq = null;
		try {
			sentDlq = this.kafkaTemplate.send(producerRecord);
			sentDlq.addCallback(new ListenableFutureCallback<SendResult<byte[],byte[]>>() {

				@Override
				public void onFailure(Throwable ex) {
					KafkaStreamsDlqDispatch.this.logger.error(
							"Error sending to DLQ " + sb.toString(), ex);
				}

				@Override
				public void onSuccess(SendResult<byte[],byte[]> result) {
					if (KafkaStreamsDlqDispatch.this.logger.isDebugEnabled()) {
						KafkaStreamsDlqDispatch.this.logger.debug(
								"Sent to DLQ " + sb.toString());
					}
				}
			});
		}
		catch (Exception ex) {
			if (sentDlq == null) {
				KafkaStreamsDlqDispatch.this.logger.error(
						"Error sending to DLQ " + sb.toString(), ex);
			}
		}
	}

	private DefaultKafkaProducerFactory<byte[],byte[]> getProducerFactory(ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
																		KafkaBinderConfigurationProperties configurationProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.ACKS_CONFIG, configurationProperties.getRequiredAcks());
		Map<String, Object> mergedConfig = configurationProperties.mergedProducerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getKafkaConnectionString());
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG,
					String.valueOf(producerProperties.getExtension().getBufferSize()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
			props.put(ProducerConfig.LINGER_MS_CONFIG,
					String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
					producerProperties.getExtension().getCompressionType().toString());
		}
		if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
			props.putAll(producerProperties.getExtension().getConfiguration());
		}
		//Always send as byte[] on dlq (the same byte[] that the consumer received)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

		return new DefaultKafkaProducerFactory<>(props);
	}

	private String toDisplayString(String original) {
		if (original.length() <= 50) {
			return original;
		}
		return original.substring(0, 50) + "...";
	}
}
