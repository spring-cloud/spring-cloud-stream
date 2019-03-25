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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.core.FluxedFunction;
import org.springframework.cloud.stream.binder.ConsumerProperties;
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
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
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
	}

	private Map<String, ResolvableType> buildTypeMap(ResolvableType resolvableType) {
		final Set<String> inputs = new TreeSet<>(this.bindableProxyFactory.getInputs());

		Map<String, ResolvableType> map = new LinkedHashMap<>();
		final Iterator<String> iterator = inputs.iterator();

		if (iterator.hasNext()) {
			map.put(iterator.next(), resolvableType.getGeneric(0));
			ResolvableType generic = resolvableType.getGeneric(1);

			while (iterator.hasNext() && generic != null) {
				if (generic.getRawClass() != null &&
						(generic.getRawClass().equals(Function.class) ||
								generic.getRawClass().equals(Consumer.class))) {
					map.put(iterator.next(), generic.getGeneric(0));
				}
				generic = generic.getGeneric(1);
			}
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	public void orchestrateStreamListenerSetupMethod(ResolvableType resolvableType, String functionName) {
		final Set<String> outputs = new TreeSet<>(this.bindableProxyFactory.getOutputs());

		String[] methodAnnotatedOutboundNames = new String[outputs.size()];
		int j = 0;
		for (String output : outputs) {
			methodAnnotatedOutboundNames[j++] = output;
		}

		final Map<String, ResolvableType> stringResolvableTypeMap = buildTypeMap(resolvableType);
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(stringResolvableTypeMap, "foobar");
		try {
			if (resolvableType.getRawClass() != null && resolvableType.getRawClass().equals(Consumer.class)) {
				Consumer<Object> consumer = functionCatalog.lookup(Consumer.class, functionName);
				consumer.accept(adaptedInboundArguments[0]);
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
					if (result.getClass().isArray()) {
						Assert.isTrue(methodAnnotatedOutboundNames.length == ((Object[]) result).length,
								"Result does not match with the number of declared outbounds");
					}
					else {
						Assert.isTrue(methodAnnotatedOutboundNames.length == 1,
								"Result does not match with the number of declared outbounds");
					}
					if (result.getClass().isArray()) {
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
						Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[0]);

						KStreamBoundElementFactory.KStreamWrapper
								boundElement = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
						boundElement.wrap((KStream) result);
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
				enableNativeDecodingForKTableAlways(parameterType, bindingProperties);
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
					//KafkaStreamsStateStoreProperties spec = buildStateStoreSpec(method);
					Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties);
					Serde<?> valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(
							bindingProperties.getConsumer(), extendedConsumerProperties);

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

	private void enableNativeDecodingForKTableAlways(Class<?> parameterType, BindingProperties bindingProperties) {
		if (parameterType.isAssignableFrom(KTable.class) || parameterType.isAssignableFrom(GlobalKTable.class)) {
			if (bindingProperties.getConsumer() == null) {
				bindingProperties.setConsumer(new ConsumerProperties());
			}
			//No framework level message conversion provided for KTable/GlobalKTable, its done by the broker.
			bindingProperties.getConsumer().setUseNativeDecoding(true);
		}
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
