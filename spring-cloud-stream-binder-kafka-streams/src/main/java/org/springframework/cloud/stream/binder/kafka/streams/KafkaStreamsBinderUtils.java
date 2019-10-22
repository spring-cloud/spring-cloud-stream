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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Common methods used by various Kafka Streams types across the binders.
 *
 * @author Soby Chacko
 * @author Gary Russell
 */
final class KafkaStreamsBinderUtils {

	private KafkaStreamsBinderUtils() {

	}

	static void prepareConsumerBinding(String name, String group,
			ApplicationContext context, KafkaTopicProvisioner kafkaTopicProvisioner,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
			ExtendedConsumerProperties<KafkaStreamsConsumerProperties> properties) {

		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				properties.getExtension());
		if (binderConfigurationProperties
				.getSerdeError() == KafkaStreamsBinderConfigurationProperties.SerdeError.sendToDlq) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}

		String[] inputTopics = StringUtils.commaDelimitedListToStringArray(name);
		for (String inputTopic : inputTopics) {
			kafkaTopicProvisioner.provisionConsumerDestination(inputTopic, group,
					extendedConsumerProperties);
		}

		if (extendedConsumerProperties.getExtension().isEnableDlq()) {

			Map<String, DlqPartitionFunction> partitionFunctions =
					context.getBeansOfType(DlqPartitionFunction.class, false, false);
			DlqPartitionFunction partitionFunction = partitionFunctions.size() == 1
					? partitionFunctions.values().iterator().next()
					: (grp, rec, ex) -> rec.partition();

			ProducerFactory<byte[], byte[]> producerFactory = getProducerFactory(
					new ExtendedProducerProperties<>(
							extendedConsumerProperties.getExtension().getDlqProducerProperties()),
					binderConfigurationProperties);
			KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);


			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver =
					(cr, e) -> new TopicPartition(extendedConsumerProperties.getExtension().getDlqName(),
							partitionFunction.apply(group, cr, e));
			DeadLetterPublishingRecoverer kafkaStreamsBinderDlqRecoverer = !StringUtils
					.isEmpty(extendedConsumerProperties.getExtension().getDlqName())
					? new DeadLetterPublishingRecoverer(kafkaTemplate, destinationResolver)
					: null;
			for (String inputTopic : inputTopics) {
				if (StringUtils.isEmpty(
						extendedConsumerProperties.getExtension().getDlqName())) {
					destinationResolver = (cr, e) -> new TopicPartition("error." + inputTopic + "." + group,
									partitionFunction.apply(group, cr, e));
					kafkaStreamsBinderDlqRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
							destinationResolver);
				}

				SendToDlqAndContinue sendToDlqAndContinue = context
						.getBean(SendToDlqAndContinue.class);
				sendToDlqAndContinue.addKStreamDlqDispatch(inputTopic,
						kafkaStreamsBinderDlqRecoverer);
			}
		}
	}

	private static DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
			KafkaBinderConfigurationProperties configurationProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.ACKS_CONFIG, configurationProperties.getRequiredAcks());
		Map<String, Object> mergedConfig = configurationProperties
				.mergedProducerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					configurationProperties.getKafkaConnectionString());
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
		// Always send as byte[] on dlq (the same byte[] that the consumer received)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				ByteArraySerializer.class);

		return new DefaultKafkaProducerFactory<>(props);
	}


	static boolean supportsKStream(MethodParameter methodParameter, Class<?> targetBeanClass) {
		return KStream.class.isAssignableFrom(targetBeanClass)
				&& KStream.class.isAssignableFrom(methodParameter.getParameterType());
	}
}
