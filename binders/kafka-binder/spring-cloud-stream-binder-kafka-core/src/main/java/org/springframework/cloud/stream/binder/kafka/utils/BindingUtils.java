/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties.StandardHeaders;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.JsonKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Binding Utilities.
 *
 * @author Gary Russell
 * @since 4.0
 *
 */
public final class BindingUtils {

	private BindingUtils() {
	}

	/**
	 * Get the message converter for consumer bindings from the application context. If
	 * the binding properties do not contain a bean name, a default
	 * {@link MessagingMessageConverter} is returned; if the binder properties contain a
	 * header mapper bean name, it is used in the default converter, otherwise a
	 * {@link JsonKafkaHeaderMapper} is used.
	 * @param applicationContext the application context.
	 * @param extendedConsumerProperties the consumer binding properties.
	 * @param configurationProperties the binder properties.
	 * @return the converter
	 * @throws IllegalStateException if a bean name is specified but not found.
	 */
	public static MessageConverter getConsumerMessageConverter(ApplicationContext applicationContext,
			ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			KafkaBinderConfigurationProperties configurationProperties) {

		MessageConverter messageConverter;
		if (extendedConsumerProperties.getExtension().getConverterBeanName() == null) {
			MessagingMessageConverter mmc = new MessagingMessageConverter();
			StandardHeaders standardHeaders = extendedConsumerProperties.getExtension()
					.getStandardHeaders();
			mmc.setGenerateMessageId(StandardHeaders.id.equals(standardHeaders)
							|| StandardHeaders.both.equals(standardHeaders));
			mmc.setGenerateTimestamp(
					StandardHeaders.timestamp.equals(standardHeaders)
							|| StandardHeaders.both.equals(standardHeaders));
			KafkaHeaderMapper headerMapper = getHeaderMapper(applicationContext, configurationProperties);
			if (headerMapper == null) {
				headerMapper = new JsonKafkaHeaderMapper();
			}
			mmc.setHeaderMapper(headerMapper);
			messageConverter = mmc;
		}
		else {
			try {
				messageConverter = applicationContext.getBean(
						extendedConsumerProperties.getExtension().getConverterBeanName(), MessageConverter.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new IllegalStateException(
						"Converter bean not present in application context", ex);
			}
		}
		return messageConverter;
	}

	/**
	 * Get the header mapper bean, if the binder properties contains a bean name; if not
	 * look for a bean with name  {@code kafkaBinderHeaderMapper} is looked up; if that
	 * doesn't exist, null is returned.
	 * @param applicationContext the application context.
	 * @param configurationProperties the binder properties.
	 * @return the mapper.
	 */
	@Nullable
	public static KafkaHeaderMapper getHeaderMapper(ApplicationContext applicationContext,
			KafkaBinderConfigurationProperties configurationProperties) {

		KafkaHeaderMapper mapper = null;
		if (configurationProperties.getHeaderMapperBeanName() != null) {
			mapper = applicationContext.getBean(
					configurationProperties.getHeaderMapperBeanName(),
					KafkaHeaderMapper.class);
		}
		if (mapper == null) {
			//First, try to see if there is a bean named headerMapper registered by other frameworks using the binder (for e.g. spring cloud sleuth)
			try {
				mapper = applicationContext.getBean("kafkaBinderHeaderMapper", KafkaHeaderMapper.class);
			}
			catch (BeansException be) {
			}
		}
		return mapper;
	}

	/**
	 * Create the Kafka configuration map for a consumer binding. With anonymous bindings
	 * (those without a {@code group} property, which are given a {@code UUID.toString()}
	 * in the group id) consumption begins from the current end of the topic, otherwise
	 * consumption starts from the beginning, the first time the binding consumes.
	 * @param anonymous true if this is for an anonymous binding.
	 * @param consumerGroup the group.
	 * @param consumerProperties the binding properties.
	 * @param configurationProperties the binder properties.
	 * @return the config map.
	 */
	public static Map<String, Object> createConsumerConfigs(boolean anonymous, String consumerGroup,
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties,
			KafkaBinderConfigurationProperties configurationProperties) {

		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

		Map<String, Object> mergedConfig = configurationProperties.mergedConsumerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					configurationProperties.getKafkaConnectionString());
		}
		Map<String, String> config = consumerProperties.getExtension().getConfiguration();
		if (!ObjectUtils.isEmpty(config)) {
			Assert.state(!config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
							+ "use multiple binders instead");
			props.putAll(config);
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getStartOffset())) {
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					consumerProperties.getExtension().getStartOffset().name());
		}
		return props;
	}

	/**
	 * Create the Kafka configuration map for a producer binding.
	 * @param producerProperties the binding properties.
	 * @param configurationProperties the binder properties.
	 * @return the config map.
	 */
	public static Map<String, Object> createProducerConfigs(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
			KafkaBinderConfigurationProperties configurationProperties) {

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG,
				String.valueOf(configurationProperties.getRequiredAcks()));
		Map<String, Object> mergedConfig = configurationProperties
				.mergedProducerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					configurationProperties.getKafkaConnectionString());
		}
		final KafkaProducerProperties kafkaProducerProperties = producerProperties.getExtension();
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG,
					String.valueOf(kafkaProducerProperties.getBufferSize()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
			props.put(ProducerConfig.LINGER_MS_CONFIG,
					String.valueOf(kafkaProducerProperties.getBatchTimeout()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
					kafkaProducerProperties.getCompressionType().toString());
		}
		Map<String, String> configs = producerProperties.getExtension().getConfiguration();
		Assert.state(!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
						+ "use multiple binders instead");
		if (!ObjectUtils.isEmpty(configs)) {
			props.putAll(configs);
		}
		if (!ObjectUtils.isEmpty(kafkaProducerProperties.getConfiguration())) {
			props.putAll(kafkaProducerProperties.getConfiguration());
		}
		return props;
	}

}
