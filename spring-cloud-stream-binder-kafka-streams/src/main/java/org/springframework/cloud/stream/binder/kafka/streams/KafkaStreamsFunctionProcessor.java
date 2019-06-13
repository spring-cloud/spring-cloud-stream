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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;

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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.core.FluxedConsumer;
import org.springframework.cloud.function.core.FluxedFunction;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
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
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
public class KafkaStreamsFunctionProcessor implements ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(KafkaStreamsFunctionProcessor.class);

	private final BindingServiceProperties bindingServiceProperties;
	private final Map<String, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();
	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;
	private final KeyValueSerdeResolver keyValueSerdeResolver;
	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;
	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;
	private final CleanupConfig cleanupConfig;
	private final FunctionCatalog functionCatalog;
	private final BindableProxyFactory bindableProxyFactory;

	private ConfigurableApplicationContext applicationContext;

	private Set<String> origInputs = new TreeSet<>();
	private Set<String> origOutputs = new TreeSet<>();

	private ResolvableType outboundResolvableType;

	public KafkaStreamsFunctionProcessor(BindingServiceProperties bindingServiceProperties,
										KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
										KeyValueSerdeResolver keyValueSerdeResolver,
										KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
										KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
										CleanupConfig cleanupConfig,
										FunctionCatalog functionCatalog,
										BindableProxyFactory bindableProxyFactory) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.cleanupConfig = cleanupConfig;
		this.functionCatalog = functionCatalog;
		this.bindableProxyFactory = bindableProxyFactory;
		this.origInputs.addAll(this.bindableProxyFactory.getInputs());
		this.origOutputs.addAll(this.bindableProxyFactory.getOutputs());
	}

	private Map<String, ResolvableType> buildTypeMap(ResolvableType resolvableType) {
		int inputCount = 1;

		ResolvableType resolvableTypeGeneric = resolvableType.getGeneric(1);
		while (resolvableTypeGeneric != null && resolvableTypeGeneric.getRawClass() != null && (resolvableTypeGeneric.getRawClass().equals(Function.class) ||
				resolvableTypeGeneric.getRawClass().equals(Consumer.class))) {
			inputCount++;
			resolvableTypeGeneric = resolvableTypeGeneric.getGeneric(1);
		}

		final Set<String> inputs = new TreeSet<>(origInputs);
		Map<String, ResolvableType> resolvableTypeMap = new LinkedHashMap<>();
		final Iterator<String> iterator = inputs.iterator();

		final String next = iterator.next();
		resolvableTypeMap.put(next, resolvableType.getGeneric(0));
		origInputs.remove(next);

		ResolvableType iterableResType = resolvableType;
		for (int i = 1; i < inputCount; i++) {
			if (iterator.hasNext()) {
				iterableResType = iterableResType.getGeneric(1);
				if (iterableResType.getRawClass() != null &&
						(iterableResType.getRawClass().equals(Function.class) ||
								iterableResType.getRawClass().equals(Consumer.class))) {
					final String next1 = iterator.next();
					resolvableTypeMap.put(next1, iterableResType.getGeneric(0));
					origInputs.remove(next1);
				}
			}
		}
		outboundResolvableType = iterableResType.getGeneric(1);
		return resolvableTypeMap;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void orchestrateFunctionInvoking(ResolvableType resolvableType, String functionName) {
		final Map<String, ResolvableType> stringResolvableTypeMap = buildTypeMap(resolvableType);
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(stringResolvableTypeMap, functionName);
		try {
			if (resolvableType.getRawClass() != null && resolvableType.getRawClass().equals(Consumer.class)) {
				FluxedConsumer fluxedConsumer = functionCatalog.lookup(FluxedConsumer.class, functionName);
				Assert.isTrue(fluxedConsumer != null,
						"No corresponding consumer beans found in the catalog");
				Object target = fluxedConsumer.getTarget();

				Consumer<Object> consumer = Consumer.class.isAssignableFrom(target.getClass()) ? (Consumer) target : null;

				if (consumer != null) {
					consumer.accept(adaptedInboundArguments[0]);
				}
			}
			else {
				Function<Object, Object> function = functionCatalog.lookup(Function.class, functionName);
				Object target = null;
				if (function instanceof FluxedFunction) {
					target = ((FluxedFunction) function).getTarget();
				}
				function = (Function) target;
				Object result = function.apply(adaptedInboundArguments[0]);
				int i = 1;
				while (result instanceof Function || result instanceof Consumer) {
					if (result instanceof Function) {
						result = ((Function) result).apply(adaptedInboundArguments[i]);
					}
					else {
						((Consumer) result).accept(adaptedInboundArguments[i]);
						result = null;
					}
					i++;
				}
				if (result != null) {
					kafkaStreamsBindingInformationCatalogue.setOutboundKStreamResolvable(
							outboundResolvableType != null ? outboundResolvableType : resolvableType.getGeneric(1));
					final Set<String> outputs = new TreeSet<>(origOutputs);
					final Iterator<String> iterator = outputs.iterator();

					if (result.getClass().isArray()) {
						final int length = ((Object[]) result).length;
						String[] methodAnnotatedOutboundNames = new String[length];

						for (int j = 0; j < length; j++) {
							if (iterator.hasNext()) {
								final String next = iterator.next();
								methodAnnotatedOutboundNames[j] = next;
								this.origOutputs.remove(next);
							}
						}
						Object[] outboundKStreams = (Object[]) result;
						int k = 0;
						for (Object outboundKStream : outboundKStreams) {
							Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[k++]);

							KStreamBoundElementFactory.KStreamWrapper
									boundElement = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
							boundElement.wrap((KStream) outboundKStream);
						}
					}
					else {
						if (iterator.hasNext()) {
							final String next = iterator.next();
							Object targetBean = this.applicationContext.getBean(next);
							this.origOutputs.remove(next);

							KStreamBoundElementFactory.KStreamWrapper
									boundElement = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
							boundElement.wrap((KStream) result);
						}
					}
				}
			}
		}
		catch (Exception ex) {
			throw new BeanInitializationException("Cannot setup StreamListener for foobar", ex);
		}
	}

	@SuppressWarnings({"unchecked"})
	private Object[] adaptAndRetrieveInboundArguments(Map<String, ResolvableType> stringResolvableTypeMap,
													String functionName) {
		Object[] arguments = new Object[stringResolvableTypeMap.size()];
		int i = 0;
		for (String input : stringResolvableTypeMap.keySet()) {
			Class<?> parameterType = stringResolvableTypeMap.get(input).getRawClass();

			if (input != null) {
				Assert.isInstanceOf(String.class, input, "Annotation value must be a String");
				Object targetBean = applicationContext.getBean(input);
				BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(input);
				//Retrieve the StreamsConfig created for this method if available.
				//Otherwise, create the StreamsBuilderFactory and get the underlying config.
				if (!this.methodStreamsBuilderFactoryBeanMap.containsKey(functionName)) {
					buildStreamsBuilderAndRetrieveConfig(functionName, applicationContext, input);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean =
							this.methodStreamsBuilderFactoryBeanMap.get(functionName);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					KafkaStreamsConsumerProperties extendedConsumerProperties =
							this.kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties(input);
					//get state store spec

					Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties, stringResolvableTypeMap.get(input));
					Serde<?> valueSerde;

					if (bindingServiceProperties.getConsumerProperties(input).isUseNativeDecoding()) {
						valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(
								bindingProperties.getConsumer(), extendedConsumerProperties, stringResolvableTypeMap.get(input));
					}
					else {
						valueSerde = Serdes.ByteArray();
					}

					final KafkaConsumerProperties.StartOffset startOffset = extendedConsumerProperties.getStartOffset();
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
						LOG.warn("Detected resetOffsets configured on binding " + input + ". "
								+ "Setting resetOffsets in Kafka Streams binder does not have any effect.");
					}

					if (parameterType.isAssignableFrom(KStream.class)) {
						KStream<?, ?> stream = getkStream(input, bindingProperties,
								streamsBuilder, keySerde, valueSerde, autoOffsetReset);
						KStreamBoundElementFactory.KStreamWrapper kStreamWrapper =
								(KStreamBoundElementFactory.KStreamWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KStream)
						kStreamWrapper.wrap((KStream<Object, Object>) stream);
						this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);

						if (KStream.class.isAssignableFrom(stringResolvableTypeMap.get(input).getRawClass())) {
							final Class<?> valueClass =
									(stringResolvableTypeMap.get(input).getGeneric(1).getRawClass() != null)
									? (stringResolvableTypeMap.get(input).getGeneric(1).getRawClass()) : Object.class;
							if (this.kafkaStreamsBindingInformationCatalogue.isUseNativeDecoding(
									(KStream) kStreamWrapper)) {
								arguments[i] = stream;
							}
							else {
								arguments[i] = this.kafkaStreamsMessageConversionDelegate.deserializeOnInbound(
										valueClass, stream);
							}
						}

						if (arguments[i] == null) {
							arguments[i] = stream;
						}
						Assert.notNull(arguments[i], "problems..");
					}
					else if (parameterType.isAssignableFrom(KTable.class)) {
						String materializedAs = extendedConsumerProperties.getMaterializedAs();
						String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
						KTable<?, ?> table = getKTable(streamsBuilder, keySerde, valueSerde, materializedAs,
								bindingDestination, autoOffsetReset);
						KTableBoundElementFactory.KTableWrapper kTableWrapper =
								(KTableBoundElementFactory.KTableWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KTable)
						kTableWrapper.wrap((KTable<Object, Object>) table);
						this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
						arguments[i] = table;
					}
					else if (parameterType.isAssignableFrom(GlobalKTable.class)) {
						String materializedAs = extendedConsumerProperties.getMaterializedAs();
						String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
						GlobalKTable<?, ?> table = getGlobalKTable(streamsBuilder, keySerde, valueSerde, materializedAs,
								bindingDestination, autoOffsetReset);
						GlobalKTableBoundElementFactory.GlobalKTableWrapper globalKTableWrapper =
								(GlobalKTableBoundElementFactory.GlobalKTableWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KTable)
						globalKTableWrapper.wrap((GlobalKTable<Object, Object>) table);
						this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
						arguments[i] = table;
					}
					i++;
				}
				catch (Exception ex) {
					throw new IllegalStateException(ex);
				}
			}
			else {
				throw new IllegalStateException(StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

	private GlobalKTable<?, ?> getGlobalKTable(StreamsBuilder streamsBuilder,
											Serde<?> keySerde, Serde<?> valueSerde, String materializedAs,
											String bindingDestination, Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null ?
				materializedAsGlobalKTable(streamsBuilder, bindingDestination, materializedAs,
						keySerde, valueSerde, autoOffsetReset) :
				streamsBuilder.globalTable(bindingDestination,
						Consumed.with(keySerde, valueSerde).withOffsetResetPolicy(autoOffsetReset));
	}

	private KTable<?, ?> getKTable(StreamsBuilder streamsBuilder, Serde<?> keySerde, Serde<?> valueSerde,
								String materializedAs,
								String bindingDestination, Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null ?
				materializedAs(streamsBuilder, bindingDestination, materializedAs, keySerde, valueSerde,
						autoOffsetReset) :
				streamsBuilder.table(bindingDestination,
						Consumed.with(keySerde, valueSerde).withOffsetResetPolicy(autoOffsetReset));
	}

	private <K, V> KTable<K, V> materializedAs(StreamsBuilder streamsBuilder, String destination,
											String storeName, Serde<K> k, Serde<V> v,
											Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.table(this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	private <K, V> GlobalKTable<K, V> materializedAsGlobalKTable(StreamsBuilder streamsBuilder,
																String destination, String storeName,
																Serde<K> k, Serde<V> v,
																Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.globalTable(this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> getMaterialized(String storeName,
																					Serde<K> k, Serde<V> v) {
		return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
				.withKeySerde(k)
				.withValueSerde(v);
	}

	private KStream<?, ?> getkStream(String inboundName,
									BindingProperties bindingProperties,
									StreamsBuilder streamsBuilder,
									Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
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

		String[] bindingTargets = StringUtils
				.commaDelimitedListToStringArray(this.bindingServiceProperties.getBindingDestination(inboundName));

		KStream<?, ?> stream =
				streamsBuilder.stream(Arrays.asList(bindingTargets),
						Consumed.with(keySerde, valueSerde)
								.withOffsetResetPolicy(autoOffsetReset));
		final boolean nativeDecoding = this.bindingServiceProperties.getConsumerProperties(inboundName)
				.isUseNativeDecoding();
		if (nativeDecoding) {
			LOG.info("Native decoding is enabled for " + inboundName + ". " +
					"Inbound deserialization done at the broker.");
		}
		else {
			LOG.info("Native decoding is disabled for " + inboundName + ". " +
					"Inbound message conversion done by Spring Cloud Stream.");
		}

		stream = stream.mapValues((value) -> {
			Object returnValue;
			String contentType = bindingProperties.getContentType();
			if (value != null && !StringUtils.isEmpty(contentType) && !nativeDecoding) {
				returnValue = MessageBuilder.withPayload(value)
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
			}
			else {
				returnValue = value;
			}
			return returnValue;
		});
		return stream;
	}

	@SuppressWarnings({"unchecked"})
	private void buildStreamsBuilderAndRetrieveConfig(String functionName, ApplicationContext applicationContext,
													String inboundName) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext.getBeanFactory();

		Map<String, Object> streamConfigGlobalProperties = applicationContext.getBean("streamConfigGlobalProperties",
				Map.class);

		KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName);
		streamConfigGlobalProperties.putAll(extendedConsumerProperties.getConfiguration());

		String applicationId = extendedConsumerProperties.getApplicationId();
		//override application.id if set at the individual binding level.
		if (StringUtils.hasText(applicationId)) {
			streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		}

		int concurrency = this.bindingServiceProperties.getConsumerProperties(inboundName).getConcurrency();
		// override concurrency if set at the individual binding level.
		if (concurrency > 1) {
			streamConfigGlobalProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, concurrency);
		}

		Map<String, KafkaStreamsDlqDispatch> kafkaStreamsDlqDispatchers = applicationContext.getBean(
				"kafkaStreamsDlqDispatchers", Map.class);

		KafkaStreamsConfiguration kafkaStreamsConfiguration =
				new KafkaStreamsConfiguration(streamConfigGlobalProperties) {
			@Override
			public Properties asProperties() {
				Properties properties = super.asProperties();
				properties.put(SendToDlqAndContinue.KAFKA_STREAMS_DLQ_DISPATCHERS, kafkaStreamsDlqDispatchers);
				return properties;
			}
		};

		StreamsBuilderFactoryBean streamsBuilder = this.cleanupConfig == null
				? new StreamsBuilderFactoryBean(kafkaStreamsConfiguration)
				: new StreamsBuilderFactoryBean(kafkaStreamsConfiguration, this.cleanupConfig);
		streamsBuilder.setAutoStartup(false);
		BeanDefinition streamsBuilderBeanDefinition =
				BeanDefinitionBuilder.genericBeanDefinition(
						(Class<StreamsBuilderFactoryBean>) streamsBuilder.getClass(), () -> streamsBuilder)
						.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("stream-builder-" +
				functionName, streamsBuilderBeanDefinition);
		StreamsBuilderFactoryBean streamsBuilderX = applicationContext.getBean("&stream-builder-" +
				functionName, StreamsBuilderFactoryBean.class);
		this.methodStreamsBuilderFactoryBeanMap.put(functionName, streamsBuilderX);
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}
}
