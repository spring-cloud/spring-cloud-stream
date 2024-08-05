/*
 * Copyright 2017-2023 the original author or authors.
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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.function.FunctionDetectorCondition;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.BinderProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Kafka Streams binder configuration.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 * @author Byungjun You
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ KafkaProperties.class, KafkaStreamsExtendedBindingProperties.class })
@ConditionalOnBean(BindingService.class)
@AutoConfigureAfter(BindingServiceConfiguration.class)
public class KafkaStreamsBinderSupportAutoConfiguration {

	private static final String KSTREAM_BINDER_TYPE = "kstream";

	private static final String KTABLE_BINDER_TYPE = "ktable";

	private static final String GLOBALKTABLE_BINDER_TYPE = "globalktable";

	private static final String CONSUMER_PROPERTIES_PREFIX = "consumer.";
	private static final String PRODUCER_PROPERTIES_PREFIX = "producer.";

	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.streams.binder")
	public KafkaStreamsBinderConfigurationProperties binderConfigurationProperties(
			KafkaProperties kafkaProperties, ConfigurableEnvironment environment,
			BindingServiceProperties properties, ConfigurableApplicationContext context) throws Exception {
		final Map<String, BinderConfiguration> binderConfigurations = getBinderConfigurations(
				properties);
		for (Map.Entry<String, BinderConfiguration> entry : binderConfigurations
				.entrySet()) {
			final BinderConfiguration binderConfiguration = entry.getValue();
			final String binderType = binderConfiguration.getBinderType();
			if (binderType != null && (binderType.equals(KSTREAM_BINDER_TYPE)
					|| binderType.equals(KTABLE_BINDER_TYPE)
					|| binderType.equals(GLOBALKTABLE_BINDER_TYPE))) {
				Map<String, Object> binderProperties = new HashMap<>();
				this.flatten(null, binderConfiguration.getProperties(), binderProperties);
				environment.getPropertySources().addFirst(
						new MapPropertySource(entry.getKey() + "-kafkaStreamsBinderEnv", binderProperties));

				Binder binder = new Binder(ConfigurationPropertySources.get(environment),
						new PropertySourcesPlaceholdersResolver(environment),
						IntegrationUtils.getConversionService(context.getBeanFactory()), null);
				final Constructor<KafkaStreamsBinderConfigurationProperties> kafkaStreamsBinderConfigurationPropertiesConstructor =
						ReflectionUtils.accessibleConstructor(KafkaStreamsBinderConfigurationProperties.class, KafkaProperties.class);
				final KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties =
						BeanUtils.instantiateClass(kafkaStreamsBinderConfigurationPropertiesConstructor, kafkaProperties);
				final BindResult<KafkaStreamsBinderConfigurationProperties> bind = binder.bind("spring.cloud.stream.kafka.streams.binder", Bindable.ofInstance(kafkaStreamsBinderConfigurationProperties));
				context.getBeanFactory().registerSingleton(
						entry.getKey() + "-KafkaStreamsBinderConfigurationProperties",
						bind.get());
			}
		}
		return new KafkaStreamsBinderConfigurationProperties(kafkaProperties);
	}

	// TODO: Lifted from core - good candidate for exposing as a utility method in core.
	private static Map<String, BinderConfiguration> getBinderConfigurations(
			BindingServiceProperties properties) {

		Map<String, BinderConfiguration> binderConfigurations = new HashMap<>();
		Map<String, BinderProperties> declaredBinders = properties.getBinders();

		for (Map.Entry<String, BinderProperties> binderEntry : declaredBinders
				.entrySet()) {
			BinderProperties binderProperties = binderEntry.getValue();
			binderConfigurations.put(binderEntry.getKey(),
					new BinderConfiguration(binderProperties.getType(),
							binderProperties.getEnvironment(),
							binderProperties.isInheritEnvironment(),
							binderProperties.isDefaultCandidate()));
		}
		return binderConfigurations;
	}

	// TODO: Lifted from core - good candidate for exposing as a utility method in core.
	private void flatten(String propertyName, Object value,
			Map<String, Object> flattenedProperties) {
		if (value instanceof Map<?, ?> valueAsMap) {
			valueAsMap.forEach((k, v) -> flatten(
					(propertyName != null ? propertyName + "." : "") + k, v,
					flattenedProperties));
		}
		else {
			flattenedProperties.put(propertyName, value.toString());
		}
	}

	@Bean
	public KafkaStreamsConfiguration kafkaStreamsConfiguration(
			@Qualifier("binderConfigurationProperties") KafkaStreamsBinderConfigurationProperties properties,
			Environment environment) {
		KafkaProperties kafkaProperties = properties.getKafkaProperties();
		Map<String, Object> streamsProperties = kafkaProperties.buildStreamsProperties(null);
		if (kafkaProperties.getStreams().getApplicationId() == null) {
			String applicationName = environment.getProperty("spring.application.name");
			if (applicationName != null) {
				streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
						applicationName);
			}
		}
		return new KafkaStreamsConfiguration(streamsProperties);
	}

	@Bean("streamConfigGlobalProperties")
	public Map<String, Object> streamConfigGlobalProperties(
			@Qualifier("binderConfigurationProperties") KafkaStreamsBinderConfigurationProperties configProperties,
			KafkaStreamsConfiguration kafkaStreamsConfiguration, ConfigurableEnvironment environment,
			SendToDlqAndContinue sendToDlqAndContinue) {

		Properties properties = kafkaStreamsConfiguration.asProperties();

		String kafkaConnectionString = configProperties.getKafkaConnectionString();

		if (kafkaConnectionString != null && kafkaConnectionString.equals("localhost:9092")) {
			//Making sure that the application indeed set a property.
			String kafkaStreamsBinderBroker = environment.getProperty("spring.cloud.stream.kafka.streams.binder.brokers");

			if (!StringUtils.hasText(kafkaStreamsBinderBroker)) {
				//Kafka Streams binder specific property for brokers is not set by the application.
				//See if there is one configured at the kafka binder level.
				String kafkaBinderBroker = environment.getProperty("spring.cloud.stream.kafka.binder.brokers");
				if (StringUtils.hasText(kafkaBinderBroker)) {
					kafkaConnectionString = kafkaBinderBroker;
					configProperties.setBrokers(kafkaConnectionString);
				}
			}
		}

		if (ObjectUtils.isEmpty(properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
					kafkaConnectionString);
		}
		else {
			Object bootstrapServerConfig = properties
					.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
			if (bootstrapServerConfig instanceof String bootStrapServers) {
				if (bootStrapServers.equals("localhost:9092")) {
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
							kafkaConnectionString);
				}
			}
			else if (bootstrapServerConfig instanceof List<?> bootStrapCollection) {
				if (bootStrapCollection.size() == 1 && bootStrapCollection.get(0).equals("localhost:9092")) {
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
							kafkaConnectionString);
				}
			}
		}

		String binderProvidedApplicationId = configProperties.getApplicationId();
		if (StringUtils.hasText(binderProvidedApplicationId)) {
			properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
					binderProvidedApplicationId);
		}

		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.ByteArraySerde.class.getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.ByteArraySerde.class.getName());

		if (configProperties
				.getDeserializationExceptionHandler() == DeserializationExceptionHandler.logAndContinue) {
			properties.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		}
		else if (configProperties
				.getDeserializationExceptionHandler() == DeserializationExceptionHandler.logAndFail) {
			properties.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		}
		else if (configProperties
				.getDeserializationExceptionHandler() == DeserializationExceptionHandler.sendToDlq) {
			properties.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					RecoveringDeserializationExceptionHandler.class);
			properties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, sendToDlqAndContinue);
		}

		if (!ObjectUtils.isEmpty(configProperties.getConfiguration())) {
			properties.putAll(configProperties.getConfiguration());
		}

		Map<String, Object> mergedConsumerConfig = new HashMap<>(configProperties.mergedConsumerConfiguration());
		//Adding consumer. prefix if they are missing (in order to differentiate them from other property categories such as stream, producer etc.)
		addPrefix(properties, mergedConsumerConfig, CONSUMER_PROPERTIES_PREFIX);

		Map<String, Object> mergedProducerConfig = new HashMap<>(configProperties.mergedProducerConfiguration());
		//Adding producer. prefix if they are missing (in order to differentiate them from other property categories such as stream, consumer etc.)
		addPrefix(properties, mergedProducerConfig, PRODUCER_PROPERTIES_PREFIX);

		if (!properties.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG)) {
			properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
					(int) configProperties.getReplicationFactor());
		}
		return properties.entrySet().stream().collect(
				Collectors.toMap((e) -> String.valueOf(e.getKey()), Map.Entry::getValue));
	}

	private void addPrefix(Properties properties, Map<String, Object> mergedConsProdConfig, String prefix) {
		Map<String, Object> mergedConfigs = new HashMap<>();
		for (String key : mergedConsProdConfig.keySet()) {
			mergedConfigs.put(key.startsWith(prefix) ? key : prefix + key, mergedConsProdConfig.get(key));
		}
		if (!ObjectUtils.isEmpty(mergedConfigs)) {
			properties.putAll(mergedConfigs);
		}
	}

	@Bean
	public KafkaStreamsMessageConversionDelegate messageConversionDelegate(
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME)
					CompositeMessageConverter compositeMessageConverter,
			SendToDlqAndContinue sendToDlqAndContinue,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
			@Qualifier("binderConfigurationProperties") KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		return new KafkaStreamsMessageConversionDelegate(compositeMessageConverter, sendToDlqAndContinue,
				KafkaStreamsBindingInformationCatalogue, binderConfigurationProperties);
	}

	@Bean
	public KStreamBoundElementFactory kStreamBoundElementFactory(
			BindingServiceProperties bindingServiceProperties,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
			EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler) {
		return new KStreamBoundElementFactory(bindingServiceProperties,
				KafkaStreamsBindingInformationCatalogue, encodingDecodingBindAdviceHandler);
	}

	@Bean
	public KTableBoundElementFactory kTableBoundElementFactory(
			BindingServiceProperties bindingServiceProperties, EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		return new KTableBoundElementFactory(bindingServiceProperties, encodingDecodingBindAdviceHandler, KafkaStreamsBindingInformationCatalogue);
	}

	@Bean
	public GlobalKTableBoundElementFactory globalKTableBoundElementFactory(
			BindingServiceProperties properties, EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		return new GlobalKTableBoundElementFactory(properties, encodingDecodingBindAdviceHandler, KafkaStreamsBindingInformationCatalogue);
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
	@ConditionalOnMissingBean
	public KeyValueSerdeResolver keyValueSerdeResolver(
			@Qualifier("streamConfigGlobalProperties") Object streamConfigGlobalProperties,
			@Qualifier("binderConfigurationProperties")KafkaStreamsBinderConfigurationProperties properties) {
		return new KeyValueSerdeResolver(
				(Map<String, Object>) streamConfigGlobalProperties, properties);
	}

	@Bean
	public InteractiveQueryService interactiveQueryServices(
			KafkaStreamsRegistry kafkaStreamsRegistry,
			@Qualifier("binderConfigurationProperties")KafkaStreamsBinderConfigurationProperties properties,
			ObjectProvider<StoreQueryParametersCustomizer<?>> storeQueryParametersCustomizerProvider) {
		InteractiveQueryService interactiveQueryService = new InteractiveQueryService(kafkaStreamsRegistry, properties);
		StoreQueryParametersCustomizer<?> storeQueryParametersCustomizer = storeQueryParametersCustomizerProvider.getIfUnique();
		if (storeQueryParametersCustomizer != null) {
			interactiveQueryService.setStoreQueryParametersCustomizer(storeQueryParametersCustomizer);
		}
		return interactiveQueryService;
	}

	@Bean
	public KafkaStreamsRegistry kafkaStreamsRegistry() {
		return new KafkaStreamsRegistry();
	}

	@Bean
	public StreamsBuilderFactoryManager streamsBuilderFactoryManager(
			KafkaStreamsBindingInformationCatalogue catalogue,
			KafkaStreamsRegistry kafkaStreamsRegistry,
			@Nullable KafkaStreamsBinderMetrics kafkaStreamsBinderMetrics,
			@Nullable KafkaStreamsMicrometerListener listener, KafkaProperties kafkaProperties) {
		return new StreamsBuilderFactoryManager(catalogue, kafkaStreamsRegistry, kafkaStreamsBinderMetrics, listener, kafkaProperties);
	}

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor(BindingServiceProperties bindingServiceProperties,
																	KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
																	KeyValueSerdeResolver keyValueSerdeResolver,
																	KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
																	KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
																	ObjectProvider<CleanupConfig> cleanupConfig,
																	StreamFunctionProperties streamFunctionProperties,
																	@Qualifier("binderConfigurationProperties") KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
																	ObjectProvider<StreamsBuilderFactoryBeanConfigurer> customizerProvider, ConfigurableEnvironment environment) {
		return new KafkaStreamsFunctionProcessor(bindingServiceProperties, kafkaStreamsExtendedBindingProperties,
				keyValueSerdeResolver, kafkaStreamsBindingInformationCatalogue, kafkaStreamsMessageConversionDelegate,
				cleanupConfig.getIfUnique(), streamFunctionProperties, kafkaStreamsBinderConfigurationProperties,
				customizerProvider.getIfUnique(), environment);
	}

	@Bean
	public EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler() {
		return new EncodingDecodingBindAdviceHandler();
	}

	@Bean
	@ConditionalOnMissingBean
	public DltPublishingContext dltSenderContext() {
		return new DltPublishingContext();
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnMissingBean(value = KafkaStreamsBinderMetrics.class, name = "outerContext")
	@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
	protected class KafkaStreamsBinderMetricsConfiguration {

		@Bean
		@ConditionalOnBean(MeterRegistry.class)
		@ConditionalOnMissingBean(KafkaStreamsBinderMetrics.class)
		@ConditionalOnMissingClass("org.springframework.kafka.core.MicrometerConsumerListener")
		public KafkaStreamsBinderMetrics kafkaStreamsBinderMetrics(MeterRegistry meterRegistry) {

			return new KafkaStreamsBinderMetrics(meterRegistry);
		}

		@ConditionalOnClass(name = "org.springframework.kafka.core.MicrometerConsumerListener")
		@ConditionalOnBean(MeterRegistry.class)
		protected class KafkaMicrometer {

			@Bean
			@ConditionalOnMissingBean(name = "binderStreamsListener")
			public KafkaStreamsMicrometerListener binderStreamsListener(MeterRegistry meterRegistry) {
				return new KafkaStreamsMicrometerListener(meterRegistry);
			}
		}
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBean(name = "outerContext")
	@ConditionalOnMissingBean(KafkaStreamsBinderMetrics.class)
	@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
	protected class KafkaStreamsBinderMetricsConfigurationWithMultiBinder {

		@Bean
		@ConditionalOnMissingClass("org.springframework.kafka.core.MicrometerConsumerListener")
		public KafkaStreamsBinderMetrics kafkaStreamsBinderMetrics(ConfigurableApplicationContext context) {

			MeterRegistry meterRegistry = context.getBean("outerContext", ApplicationContext.class)
					.getBean(MeterRegistry.class);
			return new KafkaStreamsBinderMetrics(meterRegistry);
		}

		@ConditionalOnClass(name = "org.springframework.kafka.core.MicrometerConsumerListener")
		@ConditionalOnBean(MeterRegistry.class)
		protected class KafkaMicrometer {

			@Bean
			@ConditionalOnMissingBean(name = "binderStreamsListener")
			public KafkaStreamsMicrometerListener binderStreamsListener(MeterRegistry meterRegistry) {
				return new KafkaStreamsMicrometerListener(meterRegistry);
			}
		}
	}
}
