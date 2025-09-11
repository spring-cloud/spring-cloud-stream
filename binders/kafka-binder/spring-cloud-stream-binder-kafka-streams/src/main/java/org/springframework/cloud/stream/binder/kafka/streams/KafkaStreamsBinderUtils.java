/*
 * Copyright 2018-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.utils.DlqDestinationResolver;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Common methods used by various Kafka Streams types across the binders.
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author James Forward
 */
public final class KafkaStreamsBinderUtils {

	private static final Log LOGGER = LogFactory.getLog(KafkaStreamsBinderUtils.class);

	private KafkaStreamsBinderUtils() {

	}

	/**
	 * Utility method to find the method targeted by the key.
	 *
	 * @param key name of the method
	 * @param methods collection of methods to search from
	 * @return found method as an {@link Optional}
	 */
	public static Optional<Method> findMethodWithName(String key, Method[] methods) {
		return Arrays.stream(methods).filter(m -> m.getName().equals(key) &&
			returnTypeContainsKafkaStreamsTypes(m)).findFirst();
	}

	private static boolean returnTypeContainsKafkaStreamsTypes(Method method) {
		ResolvableType resolvableType = ResolvableType.forMethodReturnType(method);
		ResolvableType[] generics = resolvableType.getGenerics();
		if (generics.length > 0) {
			Class<?> rawClass = generics[0].getRawClass();
			return rawClass != null && (rawClass.isAssignableFrom(KStream.class) || rawClass.isAssignableFrom(KTable.class)
				|| rawClass.isAssignableFrom(GlobalKTable.class));
		}
		return false;
	}

	public static String[] deriveFunctionUnits(String definition) {
		if (!StringUtils.hasText(definition)) {
			return new String[]{};
		}
		final String[] rawSplitDefinition = definition.split(";");
		return Arrays.stream(rawSplitDefinition).map(String::trim).toArray(String[]::new);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	static void prepareConsumerBinding(String name, String group,
									ApplicationContext context, KafkaTopicProvisioner kafkaTopicProvisioner,
									KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
									ExtendedConsumerProperties<KafkaStreamsConsumerProperties> properties,
									RetryTemplate retryTemplate,
									ConfigurableListableBeanFactory beanFactory, String bindingName,
									KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
									StreamsBuilderFactoryBean streamsBuilderFactoryBean) {

		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties =
				(ExtendedConsumerProperties) properties;

		if (binderConfigurationProperties
				.getDeserializationExceptionHandler() == DeserializationExceptionHandler.sendToDlq) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}
		// check for deserialization handler at the consumer binding, as that takes precedence.
		final DeserializationExceptionHandler deserializationExceptionHandler =
				properties.getExtension().getDeserializationExceptionHandler();
		if (deserializationExceptionHandler == DeserializationExceptionHandler.sendToDlq) {
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
			boolean oneFunctionPresent = partitionFunctions.size() == 1;
			Integer dlqPartitions = extendedConsumerProperties.getExtension().getDlqPartitions();
			DlqPartitionFunction partitionFunction = oneFunctionPresent
					? partitionFunctions.values().iterator().next()
					: DlqPartitionFunction.determineFallbackFunction(dlqPartitions, LOGGER);

			ProducerFactory<byte[], byte[]> producerFactory = getProducerFactory(
					new ExtendedProducerProperties<>(
							extendedConsumerProperties.getExtension().getDlqProducerProperties()),
					binderConfigurationProperties);
			kafkaStreamsBindingInformationCatalogue.addDlqProducerFactory(streamsBuilderFactoryBean, producerFactory);

			KafkaOperations<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);

			Map<String, DlqDestinationResolver> dlqDestinationResolvers =
					context.getBeansOfType(DlqDestinationResolver.class, false, false);

			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver =
					dlqDestinationResolvers.isEmpty() ? (cr, e) -> new TopicPartition(extendedConsumerProperties.getExtension().getDlqName(),
							partitionFunction.apply(group, cr, e)) :
							(cr, e) -> new TopicPartition(dlqDestinationResolvers.values().iterator().next().apply(cr, e),
									partitionFunction.apply(group, cr, e));

			DeadLetterPublishingRecoverer kafkaStreamsBinderDlqRecoverer = !dlqDestinationResolvers.isEmpty() || StringUtils
					.hasText(extendedConsumerProperties.getExtension().getDlqName())
					? new DeadLetterPublishingRecoverer(kafkaTemplate, destinationResolver)
					: null;
			for (String inputTopic : inputTopics) {
				if (!StringUtils.hasText(
						extendedConsumerProperties.getExtension().getDlqName()) && dlqDestinationResolvers.isEmpty()) {
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

		if (!StringUtils.hasText(properties.getRetryTemplateName())) {
			@SuppressWarnings("unchecked")
			BeanDefinition retryTemplateBeanDefinition = BeanDefinitionBuilder
					.genericBeanDefinition(
							(Class<RetryTemplate>) retryTemplate.getClass(),
							() -> retryTemplate)
					.getRawBeanDefinition();
			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(bindingName + "-RetryTemplate", retryTemplateBeanDefinition);
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
		Map<String, String> configs = producerProperties.getExtension().getConfiguration();
		Assert.state(!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
						+ "use multiple binders instead");
		if (!ObjectUtils.isEmpty(configs)) {
			props.putAll(configs);
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

	static void closeDlqProducerFactories(KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
										StreamsBuilderFactoryBean streamsBuilderFactoryBean) {

		final List<ProducerFactory<byte[], byte[]>> dlqProducerFactories =
				kafkaStreamsBindingInformationCatalogue.getDlqProducerFactory(streamsBuilderFactoryBean);

		if (!CollectionUtils.isEmpty(dlqProducerFactories)) {
			for (ProducerFactory<byte[], byte[]> producerFactory : dlqProducerFactories) {
				try {
					((DisposableBean) producerFactory).destroy();
				}
				catch (Exception exception) {
					throw new IllegalStateException(exception);
				}
			}
		}
	}

}
