/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 * @since 3.0.0
 */
public abstract class AbstractKafkaStreamsBinderProcessor implements ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(AbstractKafkaStreamsBinderProcessor.class);

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final BindingServiceProperties bindingServiceProperties;

	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;

	private final CleanupConfig cleanupConfig;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	protected ConfigurableApplicationContext applicationContext;

	public AbstractKafkaStreamsBinderProcessor(BindingServiceProperties bindingServiceProperties,
			KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
			KeyValueSerdeResolver keyValueSerdeResolver, CleanupConfig cleanupConfig) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.cleanupConfig = cleanupConfig;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	protected Topology.AutoOffsetReset getAutoOffsetReset(String inboundName, KafkaStreamsConsumerProperties extendedConsumerProperties) {
		final KafkaConsumerProperties.StartOffset startOffset = extendedConsumerProperties
				.getStartOffset();
		Topology.AutoOffsetReset autoOffsetReset = null;
		if (startOffset != null) {
			autoOffsetReset = switch (startOffset) {
				case earliest -> Topology.AutoOffsetReset.EARLIEST;
				case latest -> Topology.AutoOffsetReset.LATEST;
			};
		}
		if (extendedConsumerProperties.isResetOffsets()) {
			AbstractKafkaStreamsBinderProcessor.LOG.warn("Detected resetOffsets configured on binding "
					+ inboundName + ". "
					+ "Setting resetOffsets in Kafka Streams binder does not have any effect.");
		}
		return autoOffsetReset;
	}

	@SuppressWarnings("unchecked")
	protected void handleKTableGlobalKTableInputs(Object[] arguments, int index, String input, Class<?> parameterType, Object targetBean,
			StreamsBuilderFactoryBean streamsBuilderFactoryBean, StreamsBuilder streamsBuilder,
			KafkaStreamsConsumerProperties extendedConsumerProperties,
			Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset, boolean firstBuild) {
		if (firstBuild) {
			addStateStoreBeans(streamsBuilder);
		}
		if (parameterType.isAssignableFrom(KTable.class)) {
			String materializedAs = extendedConsumerProperties.getMaterializedAs();
			String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
			KTable<?, ?> table = getKTable(extendedConsumerProperties, streamsBuilder, keySerde, valueSerde, materializedAs,
					bindingDestination, autoOffsetReset);
			KTableBoundElementFactory.KTableWrapper kTableWrapper =
					(KTableBoundElementFactory.KTableWrapper) targetBean;
			//wrap the proxy created during the initial target type binding with real object (KTable)
			kTableWrapper.wrap((KTable<Object, Object>) table);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactoryPerBinding(input, streamsBuilderFactoryBean);
			this.kafkaStreamsBindingInformationCatalogue.addConsumerPropertiesPerSbfb(streamsBuilderFactoryBean,
					bindingServiceProperties.getConsumerProperties(input));
			arguments[index] = table;
		}
		else if (parameterType.isAssignableFrom(GlobalKTable.class)) {
			String materializedAs = extendedConsumerProperties.getMaterializedAs();
			String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
			GlobalKTable<?, ?> table = getGlobalKTable(extendedConsumerProperties, streamsBuilder, keySerde, valueSerde, materializedAs,
					bindingDestination, autoOffsetReset);
			GlobalKTableBoundElementFactory.GlobalKTableWrapper globalKTableWrapper =
					(GlobalKTableBoundElementFactory.GlobalKTableWrapper) targetBean;
			//wrap the proxy created during the initial target type binding with real object (KTable)
			globalKTableWrapper.wrap((GlobalKTable<Object, Object>) table);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactoryPerBinding(input, streamsBuilderFactoryBean);
			this.kafkaStreamsBindingInformationCatalogue.addConsumerPropertiesPerSbfb(streamsBuilderFactoryBean,
					bindingServiceProperties.getConsumerProperties(input));
			arguments[index] = table;
		}
	}

	@SuppressWarnings({ "unchecked" })
	protected StreamsBuilderFactoryBean buildStreamsBuilderAndRetrieveConfig(String beanNamePostPrefix,
																			ApplicationContext applicationContext, String inboundName,
																			KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
																			StreamsBuilderFactoryBeanConfigurer customizer,
																			ConfigurableEnvironment environment, BindingProperties bindingProperties) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext
				.getBeanFactory();

		Map<String, Object> streamConfigGlobalProperties = applicationContext
				.getBean("streamConfigGlobalProperties", Map.class);

		// Use a copy because the global configuration will be shared by multiple processors.
		Map<String, Object> streamConfiguration = new HashMap<>(streamConfigGlobalProperties);

		if (kafkaStreamsBinderConfigurationProperties != null) {
			final Map<String, KafkaStreamsBinderConfigurationProperties.Functions> functionConfigMap = kafkaStreamsBinderConfigurationProperties.getFunctions();
			if (!CollectionUtils.isEmpty(functionConfigMap)) {
				final KafkaStreamsBinderConfigurationProperties.Functions functionConfig = functionConfigMap.get(beanNamePostPrefix);
				if (functionConfig != null) {
					final Map<String, String> functionSpecificConfig = functionConfig.getConfiguration();
					if (!CollectionUtils.isEmpty(functionSpecificConfig)) {
						streamConfiguration.putAll(functionSpecificConfig);
					}

					String applicationId = functionConfig.getApplicationId();
					if (StringUtils.hasText(applicationId)) {
						streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
					}
				}
			}
		}

		final MutablePropertySources propertySources = environment.getPropertySources();

		if (StringUtils.hasText(bindingProperties.getBinder())) {
			final KafkaStreamsBinderConfigurationProperties multiBinderKafkaStreamsBinderConfigurationProperties =
					applicationContext.getBean(bindingProperties.getBinder() + "-KafkaStreamsBinderConfigurationProperties", KafkaStreamsBinderConfigurationProperties.class);
			String connectionString = multiBinderKafkaStreamsBinderConfigurationProperties.getKafkaConnectionString();
			if (!StringUtils.hasText(connectionString)) {
				connectionString = (String) propertySources.get(bindingProperties.getBinder() + "-kafkaStreamsBinderEnv").getProperty("spring.cloud.stream.kafka.binder.brokers");
			}

			streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString);

			String binderProvidedApplicationId = multiBinderKafkaStreamsBinderConfigurationProperties.getApplicationId();
			if (StringUtils.hasText(binderProvidedApplicationId)) {
				streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
						binderProvidedApplicationId);
			}

			if (multiBinderKafkaStreamsBinderConfigurationProperties
					.getDeserializationExceptionHandler() == DeserializationExceptionHandler.logAndContinue) {
				streamConfiguration.put(
						StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
						LogAndContinueExceptionHandler.class);
			}
			else if (multiBinderKafkaStreamsBinderConfigurationProperties
					.getDeserializationExceptionHandler() == DeserializationExceptionHandler.logAndFail) {
				streamConfiguration.put(
						StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
						LogAndFailExceptionHandler.class);
			}
			else if (multiBinderKafkaStreamsBinderConfigurationProperties
					.getDeserializationExceptionHandler() == DeserializationExceptionHandler.sendToDlq) {
				streamConfiguration.put(
						StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
						RecoveringDeserializationExceptionHandler.class);
				SendToDlqAndContinue sendToDlqAndContinue = applicationContext.getBean(SendToDlqAndContinue.class);
				streamConfiguration.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, sendToDlqAndContinue);
			}

			if (!ObjectUtils.isEmpty(multiBinderKafkaStreamsBinderConfigurationProperties.getConfiguration())) {
				streamConfiguration.putAll(multiBinderKafkaStreamsBinderConfigurationProperties.getConfiguration());
			}
			if (!streamConfiguration.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG)) {
				streamConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
						(int) multiBinderKafkaStreamsBinderConfigurationProperties.getReplicationFactor());
			}
		}

		//this is only used primarily for StreamListener based processors. Although in theory, functions can use it,
		//it is ideal for functions to use the approach used in the above if statement by using a property like
		//spring.cloud.stream.kafka.streams.binder.functions.process.configuration.num.threads (assuming that process is the function name).
		KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName);
		Map<String, String> bindingConfig = extendedConsumerProperties.getConfiguration();
		Assert.state(!bindingConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
						+ "use multiple binders instead");
		// We will only add the per binding configuration to the current streamConfiguration and not the global one.
		streamConfiguration
				.putAll(bindingConfig);

		String bindingLevelApplicationId = extendedConsumerProperties.getApplicationId();
		// override application.id if set at the individual binding level.
		// We provide this for backward compatibility with StreamListener based processors.
		// For function based processors see the approach used above
		// (i.e. use a property like spring.cloud.stream.kafka.streams.binder.functions.process.applicationId).
		if (StringUtils.hasText(bindingLevelApplicationId)) {
			streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
					bindingLevelApplicationId);
		}

		//If the application id is not set by any mechanism, then generate it.
		streamConfiguration.computeIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,
				k -> {
					String generatedApplicationID = beanNamePostPrefix + "-applicationId";
					LOG.info("Binder Generated Kafka Streams Application ID: " + generatedApplicationID);
					LOG.info("Use the binder generated application ID only for development and testing. ");
					LOG.info("For production deployments, please consider explicitly setting an application ID using a configuration property.");
					LOG.info("The generated applicationID is static and will be preserved over application restarts.");
					return generatedApplicationID;
				});

		handleConcurrency(applicationContext, inboundName, streamConfiguration);

		// Override deserialization exception handlers per binding
		final DeserializationExceptionHandler deserializationExceptionHandler =
				extendedConsumerProperties.getDeserializationExceptionHandler();
		if (deserializationExceptionHandler == DeserializationExceptionHandler.logAndFail) {
			streamConfiguration.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		}
		else if (deserializationExceptionHandler == DeserializationExceptionHandler.logAndContinue) {
			streamConfiguration.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		}
		else if (deserializationExceptionHandler == DeserializationExceptionHandler.sendToDlq) {
			streamConfiguration.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					RecoveringDeserializationExceptionHandler.class);
			streamConfiguration.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
					applicationContext.getBean(SendToDlqAndContinue.class));
		}
		else if (deserializationExceptionHandler == DeserializationExceptionHandler.skipAndContinue) {
			streamConfiguration.put(
					StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					SkipAndContinueExceptionHandler.class);
		}

		KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(streamConfiguration);

		StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.cleanupConfig == null
				? new StreamsBuilderFactoryBean(kafkaStreamsConfiguration)
				: new StreamsBuilderFactoryBean(kafkaStreamsConfiguration,
				this.cleanupConfig);

		streamsBuilderFactoryBean.setAutoStartup(false);
		BeanDefinition streamsBuilderBeanDefinition = BeanDefinitionBuilder
				.genericBeanDefinition(
						(Class<StreamsBuilderFactoryBean>) streamsBuilderFactoryBean.getClass(),
						() -> streamsBuilderFactoryBean)
				.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(
				"stream-builder-" + beanNamePostPrefix, streamsBuilderBeanDefinition);

		extendedConsumerProperties.setApplicationId((String) streamConfiguration.get(StreamsConfig.APPLICATION_ID_CONFIG));

		final StreamsBuilderFactoryBean streamsBuilderFactoryBeanFromContext = applicationContext.getBean(
				"&stream-builder-" + beanNamePostPrefix, StreamsBuilderFactoryBean.class);
		//At this point, the StreamsBuilderFactoryBean is created. If the users call, getObject()
		//in the customizer, that should grant access to the StreamsBuilder.
		if (customizer != null) {
			customizer.configure(streamsBuilderFactoryBean);
		}
		return streamsBuilderFactoryBeanFromContext;
	}

	private void handleConcurrency(ApplicationContext applicationContext, String inboundName,
								Map<String, Object> streamConfiguration) {
		// This rebinding is necessary to capture the concurrency explicitly set by the application.
		// This is added to fix this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/899
		org.springframework.boot.context.properties.bind.Binder explicitConcurrencyResolver =
				new org.springframework.boot.context.properties.bind.Binder(ConfigurationPropertySources.get(applicationContext.getEnvironment()),
						new PropertySourcesPlaceholdersResolver(applicationContext.getEnvironment()),
						IntegrationUtils.getConversionService(this.applicationContext.getBeanFactory()), null);

		boolean[] concurrencyExplicitlyProvided = new boolean[] {false};
		BindHandler handler = new BindHandler() {

			@Override
			public Object onSuccess(ConfigurationPropertyName name, Bindable<?> target,
									BindContext context, Object result) {
				if (!concurrencyExplicitlyProvided[0]) {
					concurrencyExplicitlyProvided[0] = name.getLastElement(ConfigurationPropertyName.Form.UNIFORM).equals("concurrency") &&
						// name is normalized to contain only uniform elements and thus safe to call toLowerCase here.
						ConfigurationPropertyName.of("spring.cloud.stream.bindings." + inboundName.toLowerCase() + ".consumer")
							.isAncestorOf(name);
				}
				return result;
			}
		};
		//Re-bind spring.cloud.stream properties to check if the application explicitly provided concurrency.
		try {
			explicitConcurrencyResolver.bind("spring.cloud.stream",
					Bindable.ofInstance(new BindingServiceProperties()), handler);
		}
		catch (Exception e) {
			// Ignore this exception
		}

		int concurrency = this.bindingServiceProperties.getConsumerProperties(inboundName)
				.getConcurrency();
		// override concurrency if set at the individual binding level.
		// Concurrency will be mapped to num.stream.threads.
		// This conditional also takes into account explicit concurrency settings left at the default value of 1
		// by the application to address concurrency behavior in applications with multiple processors.
		// See this GH issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/844
		if (concurrency >= 1 && concurrencyExplicitlyProvided[0]) {
			streamConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
					concurrency);
		}
	}

	protected Serde<?> getValueSerde(String inboundName, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties, ResolvableType resolvableType) {
		if (bindingServiceProperties.getConsumerProperties(inboundName).isUseNativeDecoding()) {
			BindingProperties bindingProperties = this.bindingServiceProperties
					.getBindingProperties(inboundName);
			return this.keyValueSerdeResolver.getInboundValueSerde(
					bindingProperties.getConsumer(), kafkaStreamsConsumerProperties, resolvableType);
		}
		else {
			return Serdes.ByteArray();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	protected KStream<?, ?> getKStream(String inboundName, BindingProperties bindingProperties, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
									StreamsBuilder streamsBuilder, Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset, boolean firstBuild) {
		if (firstBuild) {
			addStateStoreBeans(streamsBuilder);
		}

		final boolean nativeDecoding = this.bindingServiceProperties
				.getConsumerProperties(inboundName).isUseNativeDecoding();
		if (nativeDecoding) {
			LOG.info("Native decoding is enabled for " + inboundName
					+ ". Inbound deserialization done at the broker.");
		}
		else {
			LOG.info("Native decoding is disabled for " + inboundName
					+ ". Inbound message conversion done by Spring Cloud Stream.");
		}

		KStream<?, ?> stream;
		final Serde<?> valueSerdeToUse = StringUtils.hasText(kafkaStreamsConsumerProperties.getEventTypes()) ?
				new Serdes.BytesSerde() : valueSerde;
		final Consumed<?, ?> consumed = getConsumed(kafkaStreamsConsumerProperties, keySerde, valueSerdeToUse, autoOffsetReset);

		if (this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName).isDestinationIsPattern()) {
			final Pattern pattern = Pattern.compile(this.bindingServiceProperties.getBindingDestination(inboundName));

			stream = streamsBuilder.stream(pattern, consumed);
		}
		else {
			String[] bindingTargets = StringUtils.commaDelimitedListToStringArray(
					this.bindingServiceProperties.getBindingDestination(inboundName));
			stream = streamsBuilder.stream(Arrays.asList(bindingTargets),
					consumed);
		}
		//Check to see if event type based routing is enabled.
		//See this issue for more context: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1003
		if (StringUtils.hasText(kafkaStreamsConsumerProperties.getEventTypes())) {
			AtomicBoolean matched = new AtomicBoolean();
			AtomicReference<String> topicObject = new AtomicReference<>();
			AtomicReference<Headers> headersObject = new AtomicReference<>();
			// Processor to retrieve the header value.
			stream.process(() -> eventTypeProcessor(kafkaStreamsConsumerProperties, matched, topicObject, headersObject));
			// Branching based on event type match.
			final KStream<?, ?>[] branch = stream.branch((key, value) -> matched.getAndSet(false));
			// Deserialize if we have a branch from above.
			final KStream<?, Object> deserializedKStream = branch[0].mapValues(value -> valueSerde.deserializer().deserialize(
					topicObject.get(), headersObject.get(), ((Bytes) value).get()));
			return getkStream(bindingProperties, deserializedKStream, nativeDecoding);
		}
		return getkStream(bindingProperties, stream, nativeDecoding);
	}

	private KStream<?, ?> getkStream(BindingProperties bindingProperties, KStream<?, ?> stream, boolean nativeDecoding) {
		if (!nativeDecoding) {
			AtomicReference<Headers> headersAtomicReference = new AtomicReference<>();
			stream.process((ProcessorSupplier<Object, Object, Void, Void>) () -> new Processor<Object, Object, Void, Void>() {

				@Override
				public void init(ProcessorContext<Void, Void> context) {
					Processor.super.init(context);
				}

				@Override
				public void process(Record<Object, Object> record) {
					final Headers headers = record.headers();
					headersAtomicReference.set(headers);
				}

				@Override
				public void close() {
					Processor.super.close();
				}
			});
			stream = stream.mapValues((value) -> {
				Object returnValue;
				String contentType = bindingProperties.getContentType();
				if (value != null && StringUtils.hasText(contentType)) {
					final Headers headers = headersAtomicReference.get();
					final Map<String, Object> headersMap = new HashMap<>();
					headers.forEach(header -> headersMap.put(header.key(), header.value()));
					returnValue = MessageBuilder.withPayload(value).copyHeaders(headersMap)
							.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
				}
				else {
					returnValue = value;
				}
				return returnValue;
			});
		}
		return stream;
	}

	@SuppressWarnings("rawtypes")
	private void addStateStoreBeans(StreamsBuilder streamsBuilder) {
		try {
			final Map<String, StoreBuilder> storeBuilders = applicationContext.getBeansOfType(StoreBuilder.class);
			if (!CollectionUtils.isEmpty(storeBuilders)) {
				storeBuilders.values().forEach(storeBuilder -> {
					streamsBuilder.addStateStore(storeBuilder);
					if (LOG.isInfoEnabled()) {
						LOG.info("state store " + storeBuilder.name() + " added to topology");
					}
				});
			}
		}
		catch (Exception e) {
			// Pass through.
		}
	}

	private <K, V> KTable<K, V> materializedAs(StreamsBuilder streamsBuilder, String destination, String storeName,
			Serde<K> k, Serde<V> v, Topology.AutoOffsetReset autoOffsetReset, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties) {

		final Consumed<K, V> consumed = getConsumed(kafkaStreamsConsumerProperties, k, v, autoOffsetReset);
		return streamsBuilder.table(this.bindingServiceProperties.getBindingDestination(destination),
				consumed, getMaterialized(storeName, k, v));
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> getMaterialized(
			String storeName, Serde<K> k, Serde<V> v) {
		return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
				.withKeySerde(k).withValueSerde(v);
	}

	private <K, V> GlobalKTable<K, V> materializedAsGlobalKTable(
			StreamsBuilder streamsBuilder, String destination, String storeName,
			Serde<K> k, Serde<V> v, Topology.AutoOffsetReset autoOffsetReset, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties) {
		final Consumed<K, V> consumed = getConsumed(kafkaStreamsConsumerProperties, k, v, autoOffsetReset);
		return streamsBuilder.globalTable(
				this.bindingServiceProperties.getBindingDestination(destination),
				consumed,
				getMaterialized(storeName, k, v));
	}

	private GlobalKTable<?, ?> getGlobalKTable(KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
			StreamsBuilder streamsBuilder,
			Serde<?> keySerde, Serde<?> valueSerde, String materializedAs,
			String bindingDestination, Topology.AutoOffsetReset autoOffsetReset) {
		final Consumed<?, ?> consumed = getConsumed(kafkaStreamsConsumerProperties, keySerde, valueSerde, autoOffsetReset);
		return materializedAs != null
				? materializedAsGlobalKTable(streamsBuilder, bindingDestination,
				materializedAs, keySerde, valueSerde, autoOffsetReset, kafkaStreamsConsumerProperties)
				: streamsBuilder.globalTable(bindingDestination,
				consumed);
	}

	private KTable<?, ?> getKTable(KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
			StreamsBuilder streamsBuilder, Serde<?> keySerde,
			Serde<?> valueSerde, String materializedAs, String bindingDestination,
			Topology.AutoOffsetReset autoOffsetReset) {

		final Serde<?> valueSerdeToUse = StringUtils.hasText(kafkaStreamsConsumerProperties.getEventTypes()) ?
				new Serdes.BytesSerde() : valueSerde;

		final Consumed<?, ?> consumed = getConsumed(kafkaStreamsConsumerProperties, keySerde, valueSerdeToUse, autoOffsetReset);

		final KTable<?, ?> kTable = materializedAs != null
				? materializedAs(streamsBuilder, bindingDestination, materializedAs,
				keySerde, valueSerdeToUse, autoOffsetReset, kafkaStreamsConsumerProperties)
				: streamsBuilder.table(bindingDestination,
				consumed);
		if (StringUtils.hasText(kafkaStreamsConsumerProperties.getEventTypes())) {
			AtomicBoolean matched = new AtomicBoolean();
			AtomicReference<String> topicObject = new AtomicReference<>();
			AtomicReference<Headers> headersObject = new AtomicReference<>();

			final KStream<?, ?> stream = kTable.toStream();

			// Processor to retrieve the header value.
			stream.process(() -> eventTypeProcessor(kafkaStreamsConsumerProperties, matched, topicObject, headersObject));
			// Branching based on event type match.
			final Map<String, ? extends KStream<?, ?>> stringKStreamMap = stream.split()
				.branch((key, value) -> matched.getAndSet(false))
				.noDefaultBranch();
			final KStream<?, ?>[] branch = stringKStreamMap.values().toArray(new KStream[0]);
			// Deserialize if we have a branch from above.
			final KStream<?, Object> deserializedKStream = branch[0].mapValues(value -> valueSerde.deserializer().deserialize(
					topicObject.get(), headersObject.get(), ((Bytes) value).get()));

			return deserializedKStream.toTable();
		}
		return kTable;
	}

	private <K, V> Consumed<K, V> getConsumed(KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
			Serde<K> keySerde, Serde<V> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
		TimestampExtractor timestampExtractor = null;
		if (StringUtils.hasText(kafkaStreamsConsumerProperties.getTimestampExtractorBeanName())) {
			timestampExtractor = applicationContext.getBean(kafkaStreamsConsumerProperties.getTimestampExtractorBeanName(),
					TimestampExtractor.class);
		}
		final Consumed<K, V> consumed = Consumed.with(keySerde, valueSerde)
				.withOffsetResetPolicy(autoOffsetReset);
		if (timestampExtractor != null) {
			consumed.withTimestampExtractor(timestampExtractor);
		}
		if (StringUtils.hasText(kafkaStreamsConsumerProperties.getConsumedAs())) {
			consumed.withName(kafkaStreamsConsumerProperties.getConsumedAs());
		}
		return consumed;
	}

	private <K, V> Processor<K, V, Void, Void> eventTypeProcessor(KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
																AtomicBoolean matched, AtomicReference<String> topicObject, AtomicReference<Headers> headersObject) {
		return new Processor<>() {

			org.apache.kafka.streams.processor.api.ProcessorContext<?, ?> context;

			@Override
			public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
				Processor.super.init(context);
				this.context = context;
			}

			@Override
			public void process(Record<K, V> record) {
				final Headers headers = record.headers();
				headersObject.set(headers);
				final Optional<RecordMetadata> optional = this.context.recordMetadata();
				if (optional.isPresent()) {
					final RecordMetadata recordMetadata = optional.get();
					topicObject.set(recordMetadata.topic());
				}
				final Iterable<Header> eventTypeHeader = headers.headers(kafkaStreamsConsumerProperties.getEventTypeHeaderKey());
				if (eventTypeHeader != null && eventTypeHeader.iterator().hasNext()) {
					String eventTypeFromHeader = new String(eventTypeHeader.iterator().next().value());
					final String[] eventTypesFromBinding = StringUtils.commaDelimitedListToStringArray(kafkaStreamsConsumerProperties.getEventTypes());
					for (String eventTypeFromBinding : eventTypesFromBinding) {
						if (eventTypeFromHeader.equals(eventTypeFromBinding)) {
							matched.set(true);
							break;
						}
					}
				}
			}

			@Override
			public void close() {

			}
		};
	}
}
