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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.core.FluxedConsumer;
import org.springframework.cloud.function.core.FluxedFunction;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
public class KafkaStreamsFunctionProcessor extends AbstractKafkaStreamsBinderProcessor {

	private static final Log LOG = LogFactory.getLog(KafkaStreamsFunctionProcessor.class);

	private final BindingServiceProperties bindingServiceProperties;
	private final Map<String, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();
	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;
	private final KeyValueSerdeResolver keyValueSerdeResolver;
	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;
	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;
	private final FunctionCatalog functionCatalog;

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
		super(bindingServiceProperties, kafkaStreamsBindingInformationCatalogue, kafkaStreamsExtendedBindingProperties,
				keyValueSerdeResolver, cleanupConfig);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.functionCatalog = functionCatalog;
		this.origInputs.addAll(bindableProxyFactory.getInputs());
		this.origOutputs.addAll(bindableProxyFactory.getOutputs());
	}

	private Map<String, ResolvableType> buildTypeMap(ResolvableType resolvableType) {
		int inputCount = 1;

		ResolvableType resolvableTypeGeneric = resolvableType.getGeneric(1);
		while (resolvableTypeGeneric != null && resolvableTypeGeneric.getRawClass() != null && (functionOrConsumerFound(resolvableTypeGeneric))) {
			inputCount++;
			resolvableTypeGeneric = resolvableTypeGeneric.getGeneric(1);
		}

		final Set<String> inputs = new TreeSet<>(origInputs);
		Map<String, ResolvableType> resolvableTypeMap = new LinkedHashMap<>();
		final Iterator<String> iterator = inputs.iterator();

		popuateResolvableTypeMap(resolvableType, resolvableTypeMap, iterator);

		ResolvableType iterableResType = resolvableType;
		for (int i = 1; i < inputCount; i++) {
			if (iterator.hasNext()) {
				iterableResType = iterableResType.getGeneric(1);
				if (iterableResType.getRawClass() != null &&
						functionOrConsumerFound(iterableResType)) {
					popuateResolvableTypeMap(iterableResType, resolvableTypeMap, iterator);
				}
			}
		}
		outboundResolvableType = iterableResType.getGeneric(1);
		return resolvableTypeMap;
	}

	private boolean functionOrConsumerFound(ResolvableType iterableResType) {
		return iterableResType.getRawClass().equals(Function.class) ||
				iterableResType.getRawClass().equals(Consumer.class);
	}

	private void popuateResolvableTypeMap(ResolvableType resolvableType, Map<String, ResolvableType> resolvableTypeMap, Iterator<String> iterator) {
		final String next = iterator.next();
		resolvableTypeMap.put(next, resolvableType.getGeneric(0));
		origInputs.remove(next);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setupFunctionInvokerForKafkaStreams(ResolvableType resolvableType, String functionName) {
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
				Assert.isTrue(function != null, "Function bean cannot be null");
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
					final Iterator<String> outboundDefinitionIterator = outputs.iterator();

					if (result.getClass().isArray()) {
						final int length = ((Object[]) result).length;
						String[] methodAnnotatedOutboundNames = new String[length];

						for (int j = 0; j < length; j++) {
							if (outboundDefinitionIterator.hasNext()) {
								final String next = outboundDefinitionIterator.next();
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
						if (outboundDefinitionIterator.hasNext()) {
							final String next = outboundDefinitionIterator.next();
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
			throw new BeanInitializationException("Cannot setup function invoker for this Kafka Streams function.", ex);
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
				Object targetBean = applicationContext.getBean(input);
				BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(input);
				//Retrieve the StreamsConfig created for this method if available.
				//Otherwise, create the StreamsBuilderFactory and get the underlying config.
				if (!this.methodStreamsBuilderFactoryBeanMap.containsKey(functionName)) {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = buildStreamsBuilderAndRetrieveConfig("stream-builder-" + functionName, applicationContext, input);
					this.methodStreamsBuilderFactoryBeanMap.put(functionName, streamsBuilderFactoryBean);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean =
							this.methodStreamsBuilderFactoryBeanMap.get(functionName);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					KafkaStreamsConsumerProperties extendedConsumerProperties =
							this.kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties(input);
					//get state store spec

					Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties, stringResolvableTypeMap.get(input));
					Serde<?> valueSerde = bindingServiceProperties.getConsumerProperties(input).isUseNativeDecoding() ?
						getValueSerde(input, extendedConsumerProperties, stringResolvableTypeMap.get(input)) : Serdes.ByteArray();

					final Topology.AutoOffsetReset autoOffsetReset = getAutoOffsetReset(input, extendedConsumerProperties);

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
						Assert.notNull(arguments[i], "Problems encountered while adapting the function argument.");
					}
					else {
						handleKTableGlobalKTableInputs(arguments, i, input, parameterType, targetBean, streamsBuilderFactoryBean,
								streamsBuilder, extendedConsumerProperties, keySerde, valueSerde, autoOffsetReset);
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

		return getkStream(bindingProperties, stream, nativeDecoding);
	}

}
