/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 */
@EnableConfigurationProperties(KafkaStreamsExtendedBindingProperties.class)
@ConditionalOnBean(BindingService.class)
@AutoConfigureAfter(BindingServiceConfiguration.class)
public class KafkaStreamsBinderSupportAutoConfiguration {

	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.streams.binder")
	public KafkaStreamsBinderConfigurationProperties binderConfigurationProperties(KafkaProperties kafkaProperties) {
		return new KafkaStreamsBinderConfigurationProperties(kafkaProperties);
	}

	@Bean
	public KafkaStreamsConfiguration kafkaStreamsConfiguration(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
															Environment environment) {
		KafkaProperties kafkaProperties = binderConfigurationProperties.getKafkaProperties();
		Map<String, Object> streamsProperties = kafkaProperties.buildStreamsProperties();
		if (kafkaProperties.getStreams().getApplicationId() == null) {
			String applicationName = environment.getProperty("spring.application.name");
			if (applicationName != null) {
				streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
			}
		}
		return new KafkaStreamsConfiguration(streamsProperties);
	}

	@Bean("streamConfigGlobalProperties")
	public Map<String, Object> streamConfigGlobalProperties(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
															KafkaStreamsConfiguration kafkaStreamsConfiguration) {

		Properties properties = kafkaStreamsConfiguration.asProperties();
		// Override Spring Boot bootstrap server setting if left to default with the value
		// configured in the binder
		if (ObjectUtils.isEmpty(properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, binderConfigurationProperties.getKafkaConnectionString());
		}
		else {
			Object bootstrapServerConfig = properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
			if (bootstrapServerConfig instanceof String) {
				@SuppressWarnings("unchecked")
				String bootStrapServers = (String) properties
						.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
				if (bootStrapServers.equals("localhost:9092")) {
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, binderConfigurationProperties.getKafkaConnectionString());
				}
			}
		}

		String binderProvidedApplicationId = binderConfigurationProperties.getApplicationId();
		if (StringUtils.hasText(binderProvidedApplicationId)) {
			properties.put(StreamsConfig.APPLICATION_ID_CONFIG, binderProvidedApplicationId);
		}

		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());

		if (binderConfigurationProperties.getSerdeError() == KafkaStreamsBinderConfigurationProperties.SerdeError.logAndContinue) {
			properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		} else if (binderConfigurationProperties.getSerdeError() == KafkaStreamsBinderConfigurationProperties.SerdeError.logAndFail) {
			properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		} else if (binderConfigurationProperties.getSerdeError() == KafkaStreamsBinderConfigurationProperties.SerdeError.sendToDlq) {
			properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					SendToDlqAndContinue.class);
		}

		if (!ObjectUtils.isEmpty(binderConfigurationProperties.getConfiguration())) {
			properties.putAll(binderConfigurationProperties.getConfiguration());
		}
		return properties.entrySet().stream().collect(
				Collectors.toMap(e -> String.valueOf(e.getKey()), Map.Entry::getValue));
	}

	@Bean
	public KStreamStreamListenerResultAdapter kstreamStreamListenerResultAdapter() {
		return new KStreamStreamListenerResultAdapter();
	}

	@Bean
	public KStreamStreamListenerParameterAdapter kstreamStreamListenerParameterAdapter(
			KafkaStreamsMessageConversionDelegate kstreamBoundMessageConversionDelegate, KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		return new KStreamStreamListenerParameterAdapter(kstreamBoundMessageConversionDelegate, KafkaStreamsBindingInformationCatalogue);
	}

	@Bean
	public KafkaStreamsStreamListenerSetupMethodOrchestrator kafkaStreamsStreamListenerSetupMethodOrchestrator(
			BindingServiceProperties bindingServiceProperties,
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
			KeyValueSerdeResolver keyValueSerdeResolver,
			KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
			KStreamStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter,
			Collection<StreamListenerResultAdapter> streamListenerResultAdapters,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
			ObjectProvider<CleanupConfig> cleanupConfig) {
		return new KafkaStreamsStreamListenerSetupMethodOrchestrator(bindingServiceProperties,
				kafkaStreamsExtendedBindingProperties, keyValueSerdeResolver, kafkaStreamsBindingInformationCatalogue,
				kafkaStreamListenerParameterAdapter, streamListenerResultAdapters, binderConfigurationProperties,
				cleanupConfig.getIfUnique());
	}

	@Bean
	public KafkaStreamsMessageConversionDelegate messageConversionDelegate(CompositeMessageConverterFactory compositeMessageConverterFactory,
																		SendToDlqAndContinue sendToDlqAndContinue,
																		KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
																		KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		return new KafkaStreamsMessageConversionDelegate(compositeMessageConverterFactory, sendToDlqAndContinue,
				KafkaStreamsBindingInformationCatalogue, binderConfigurationProperties);
	}

	@Bean
	public KStreamBoundElementFactory kStreamBoundElementFactory(BindingServiceProperties bindingServiceProperties,
																	KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		return new KStreamBoundElementFactory(bindingServiceProperties,
				KafkaStreamsBindingInformationCatalogue);
	}

	@Bean
	public KTableBoundElementFactory kTableBoundElementFactory(BindingServiceProperties bindingServiceProperties) {
		return new KTableBoundElementFactory(bindingServiceProperties);
	}

	@Bean
	public GlobalKTableBoundElementFactory globalKTableBoundElementFactory(BindingServiceProperties bindingServiceProperties) {
		return new GlobalKTableBoundElementFactory(bindingServiceProperties);
	}

	@Bean
	public SendToDlqAndContinue sendToDlqAndContinue() {
		return new SendToDlqAndContinue();
	}

	@Bean
	public KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue() {
		return new KafkaStreamsBindingInformationCatalogue();
	}

	@Bean
	@SuppressWarnings("unchecked")
	public KeyValueSerdeResolver keyValueSerdeResolver(@Qualifier("streamConfigGlobalProperties") Object streamConfigGlobalProperties,
													KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties) {
		return new KeyValueSerdeResolver((Map<String, Object>) streamConfigGlobalProperties, kafkaStreamsBinderConfigurationProperties);
	}

	@Bean
	public QueryableStoreRegistry queryableStoreTypeRegistry(KafkaStreamsRegistry kafkaStreamsRegistry) {
		return new QueryableStoreRegistry(kafkaStreamsRegistry);
	}

	@Bean
	public InteractiveQueryService interactiveQueryServices(KafkaStreamsRegistry kafkaStreamsRegistry,
															KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		return new InteractiveQueryService(kafkaStreamsRegistry, binderConfigurationProperties);
	}

	@Bean
	public KafkaStreamsRegistry kafkaStreamsRegistry() {
		return new KafkaStreamsRegistry();
	}

	@Bean
	public StreamsBuilderFactoryManager streamsBuilderFactoryManager(KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
																	KafkaStreamsRegistry kafkaStreamsRegistry) {
		return new StreamsBuilderFactoryManager(kafkaStreamsBindingInformationCatalogue, kafkaStreamsRegistry);
	}

}
