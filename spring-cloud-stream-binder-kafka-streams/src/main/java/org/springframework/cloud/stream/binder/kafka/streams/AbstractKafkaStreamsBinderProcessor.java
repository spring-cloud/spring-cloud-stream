/*
 * Copyright 2019-2019 the original author or authors.
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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
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
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.CollectionUtils;
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
			switch (startOffset) {
				case earliest:
					autoOffsetReset = Topology.AutoOffsetReset.EARLIEST;
					break;
				case latest:
					autoOffsetReset = Topology.AutoOffsetReset.LATEST;
					break;
				default:
					break;
			}
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
												Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
		if (parameterType.isAssignableFrom(KTable.class)) {
			String materializedAs = extendedConsumerProperties.getMaterializedAs();
			String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
			KTable<?, ?> table = getKTable(extendedConsumerProperties, streamsBuilder, keySerde, valueSerde, materializedAs,
					bindingDestination, autoOffsetReset);
			KTableBoundElementFactory.KTableWrapper kTableWrapper =
					(KTableBoundElementFactory.KTableWrapper) targetBean;
			//wrap the proxy created during the initial target type binding with real object (KTable)
			kTableWrapper.wrap((KTable<Object, Object>) table);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
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
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
			arguments[index] = table;
		}
	}

	@SuppressWarnings({"unchecked"})
	protected StreamsBuilderFactoryBean buildStreamsBuilderAndRetrieveConfig(String beanNamePostPrefix,
																			ApplicationContext applicationContext, String inboundName,
																			KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext
				.getBeanFactory();

		Map<String, Object> streamConfigGlobalProperties = applicationContext
				.getBean("streamConfigGlobalProperties", Map.class);

		if (kafkaStreamsBinderConfigurationProperties != null) {
			final Map<String, KafkaStreamsBinderConfigurationProperties.Functions> functionConfigMap = kafkaStreamsBinderConfigurationProperties.getFunctions();
			if (!CollectionUtils.isEmpty(functionConfigMap)) {
				final KafkaStreamsBinderConfigurationProperties.Functions functionConfig = functionConfigMap.get(beanNamePostPrefix);
				final Map<String, String> functionSpecificConfig = functionConfig.getConfiguration();
				if (!CollectionUtils.isEmpty(functionSpecificConfig)) {
					streamConfigGlobalProperties.putAll(functionSpecificConfig);
				}

				String applicationId = functionConfig.getApplicationId();
				if (!StringUtils.isEmpty(applicationId)) {
					streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
				}
			}
		}

		//this is only used primarily for StreamListener based processors. Although in theory, functions can use it,
		//it is ideal for functions to use the approach used in the above if statement by using a property like
		//spring.cloud.stream.kafka.streams.binder.functions.process.configuration.num.threads (assuming that process is the function name).
		KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName);
		streamConfigGlobalProperties
				.putAll(extendedConsumerProperties.getConfiguration());

		String bindingLevelApplicationId = extendedConsumerProperties.getApplicationId();
		// override application.id if set at the individual binding level.
		// We provide this for backward compatibility with StreamListener based processors.
		// For function based processors see the approach used above
		// (i.e. use a property like spring.cloud.stream.kafka.streams.binder.functions.process.applicationId).
		if (StringUtils.hasText(bindingLevelApplicationId)) {
			streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
					bindingLevelApplicationId);
		}

		//If the application id is not set by any mechanism, then generate it.
		streamConfigGlobalProperties.computeIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,
				k -> {
					String generatedApplicationID = beanNamePostPrefix + "-applicationId";
					LOG.info("Binder Generated Kafka Streams Application ID: " + generatedApplicationID);
					LOG.info("Use the binder generated application ID only for development and testing. ");
					LOG.info("For production deployments, please consider explicitly setting an application ID using a configuration property.");
					LOG.info("The generated applicationID is static and will be preserved over application restarts.");
					return generatedApplicationID;
				});

		int concurrency = this.bindingServiceProperties.getConsumerProperties(inboundName)
				.getConcurrency();
		// override concurrency if set at the individual binding level.
		if (concurrency > 1) {
			streamConfigGlobalProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
					concurrency);
		}

		KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(streamConfigGlobalProperties);

		StreamsBuilderFactoryBean streamsBuilder = this.cleanupConfig == null
				? new StreamsBuilderFactoryBean(kafkaStreamsConfiguration)
				: new StreamsBuilderFactoryBean(kafkaStreamsConfiguration,
				this.cleanupConfig);
		streamsBuilder.setAutoStartup(false);
		BeanDefinition streamsBuilderBeanDefinition = BeanDefinitionBuilder
				.genericBeanDefinition(
						(Class<StreamsBuilderFactoryBean>) streamsBuilder.getClass(),
						() -> streamsBuilder)
				.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(
				"stream-builder-" + beanNamePostPrefix, streamsBuilderBeanDefinition);

		extendedConsumerProperties.setApplicationId((String) streamConfigGlobalProperties.get(StreamsConfig.APPLICATION_ID_CONFIG));
		//Removing the application ID from global properties so that the next function won't re-use it and cause race conditions.
		streamConfigGlobalProperties.remove(StreamsConfig.APPLICATION_ID_CONFIG);

		return applicationContext.getBean(
				"&stream-builder-" + beanNamePostPrefix, StreamsBuilderFactoryBean.class);
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

	protected KStream<?, ?> getKStream(String inboundName, BindingProperties bindingProperties, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
									StreamsBuilder streamsBuilder, Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
		addStateStoreBeans(streamsBuilder);

		String[] bindingTargets = StringUtils.commaDelimitedListToStringArray(
				this.bindingServiceProperties.getBindingDestination(inboundName));
		final Consumed<?, ?> consumed = getConsumed(kafkaStreamsConsumerProperties, keySerde, valueSerde, autoOffsetReset);
		KStream<?, ?> stream = streamsBuilder.stream(Arrays.asList(bindingTargets),
				consumed);
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

		return getkStream(bindingProperties, stream, nativeDecoding);
	}

	private KStream<?, ?> getkStream(BindingProperties bindingProperties, KStream<?, ?> stream, boolean nativeDecoding) {
		if (!nativeDecoding) {
			stream = stream.mapValues((value) -> {
				Object returnValue;
				String contentType = bindingProperties.getContentType();
				if (value != null && !StringUtils.isEmpty(contentType)) {
					returnValue = MessageBuilder.withPayload(value)
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
		final Consumed<?, ?> consumed = getConsumed(kafkaStreamsConsumerProperties, keySerde, valueSerde, autoOffsetReset);
		return materializedAs != null
				? materializedAs(streamsBuilder, bindingDestination, materializedAs,
				keySerde, valueSerde, autoOffsetReset, kafkaStreamsConsumerProperties)
				: streamsBuilder.table(bindingDestination,
				consumed);
	}

	private <K, V> Consumed<K, V> getConsumed(KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties,
											Serde<K> keySerde, Serde<V> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
		TimestampExtractor timestampExtractor = null;
		if (!StringUtils.isEmpty(kafkaStreamsConsumerProperties.getTimestampExtractorBeanName())) {
			timestampExtractor = applicationContext.getBean(kafkaStreamsConsumerProperties.getTimestampExtractorBeanName(),
					TimestampExtractor.class);
		}
		final Consumed<K, V> consumed = Consumed.with(keySerde, valueSerde)
				.withOffsetResetPolicy(autoOffsetReset);
		if (timestampExtractor != null) {
			consumed.withTimestampExtractor(timestampExtractor);
		}
		return consumed;
	}
}
