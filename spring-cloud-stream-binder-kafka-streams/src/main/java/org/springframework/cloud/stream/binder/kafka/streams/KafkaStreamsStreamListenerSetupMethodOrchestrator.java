/*
 * Copyright 2018-2019 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.binding.StreamListenerSetupMethodOrchestrator;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Kafka Streams specific implementation for {@link StreamListenerSetupMethodOrchestrator}
 * that overrides the default mechanisms for invoking StreamListener adapters.
 * <p>
 * The orchestration primarily focus on the following areas:
 * <p>
 * 1. Allow multiple KStream output bindings (KStream branching) by allowing more than one
 * output values on {@link SendTo} 2. Allow multiple inbound bindings for multiple KStream
 * and or KTable/GlobalKTable types. 3. Each StreamListener method that it orchestrates
 * gets its own {@link StreamsBuilderFactoryBean} and {@link StreamsConfig}
 *
 * @author Soby Chacko
 * @author Lei Chen
 * @author Gary Russell
 */
class KafkaStreamsStreamListenerSetupMethodOrchestrator
		implements StreamListenerSetupMethodOrchestrator, ApplicationContextAware {

	private static final Log LOG = LogFactory
			.getLog(KafkaStreamsStreamListenerSetupMethodOrchestrator.class);

	private final StreamListenerParameterAdapter streamListenerParameterAdapter;

	private final Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

	private final BindingServiceProperties bindingServiceProperties;

	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final Map<Method, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();

	private final Map<Method, List<String>> registeredStoresPerMethod = new HashMap<>();

	private final CleanupConfig cleanupConfig;

	private ConfigurableApplicationContext applicationContext;

	KafkaStreamsStreamListenerSetupMethodOrchestrator(
			BindingServiceProperties bindingServiceProperties,
			KafkaStreamsExtendedBindingProperties extendedBindingProperties,
			KeyValueSerdeResolver keyValueSerdeResolver,
			KafkaStreamsBindingInformationCatalogue bindingInformationCatalogue,
			StreamListenerParameterAdapter streamListenerParameterAdapter,
			Collection<StreamListenerResultAdapter> listenerResultAdapters,
			CleanupConfig cleanupConfig) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = extendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = bindingInformationCatalogue;
		this.streamListenerParameterAdapter = streamListenerParameterAdapter;
		this.streamListenerResultAdapters = listenerResultAdapters;
		this.cleanupConfig = cleanupConfig;
	}

	@Override
	public boolean supports(Method method) {
		return methodParameterSupports(method) && (methodReturnTypeSuppports(method)
				|| Void.TYPE.equals(method.getReturnType()));
	}

	private boolean methodReturnTypeSuppports(Method method) {
		Class<?> returnType = method.getReturnType();
		if (returnType.equals(KStream.class) || (returnType.isArray()
				&& returnType.getComponentType().equals(KStream.class))) {
			return true;
		}
		return false;
	}

	private boolean methodParameterSupports(Method method) {
		boolean supports = false;
		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, i);
			Class<?> parameterType = methodParameter.getParameterType();
			if (parameterType.equals(KStream.class) || parameterType.equals(KTable.class)
					|| parameterType.equals(GlobalKTable.class)) {
				supports = true;
			}
		}
		return supports;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void orchestrateStreamListenerSetupMethod(StreamListener streamListener,
													Method method, Object bean) {
		String[] methodAnnotatedOutboundNames = getOutboundBindingTargetNames(method);
		validateStreamListenerMethod(streamListener, method,
				methodAnnotatedOutboundNames);
		String methodAnnotatedInboundName = streamListener.value();
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(method,
				methodAnnotatedInboundName, this.applicationContext,
				this.streamListenerParameterAdapter);
		try {
			ReflectionUtils.makeAccessible(method);
			if (Void.TYPE.equals(method.getReturnType())) {
				method.invoke(bean, adaptedInboundArguments);
			}
			else {
				Object result = method.invoke(bean, adaptedInboundArguments);

				if (result.getClass().isArray()) {
					Assert.isTrue(
							methodAnnotatedOutboundNames.length == ((Object[]) result).length,
							"Result does not match with the number of declared outbounds");
				}
				else {
					Assert.isTrue(methodAnnotatedOutboundNames.length == 1,
							"Result does not match with the number of declared outbounds");
				}
				if (result.getClass().isArray()) {
					Object[] outboundKStreams = (Object[]) result;
					int i = 0;
					for (Object outboundKStream : outboundKStreams) {
						Object targetBean = this.applicationContext
								.getBean(methodAnnotatedOutboundNames[i++]);
						for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
							if (streamListenerResultAdapter.supports(
									outboundKStream.getClass(), targetBean.getClass())) {
								streamListenerResultAdapter.adapt(outboundKStream,
										targetBean);
								break;
							}
						}
					}
				}
				else {
					Object targetBean = this.applicationContext
							.getBean(methodAnnotatedOutboundNames[0]);
					for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
						if (streamListenerResultAdapter.supports(result.getClass(),
								targetBean.getClass())) {
							streamListenerResultAdapter.adapt(result, targetBean);
							break;
						}
					}
				}
			}
		}
		catch (Exception ex) {
			throw new BeanInitializationException(
					"Cannot setup StreamListener for " + method, ex);
		}
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public Object[] adaptAndRetrieveInboundArguments(Method method, String inboundName,
													ApplicationContext applicationContext,
													StreamListenerParameterAdapter... adapters) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method,
					parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils
						.getValue(methodParameter.getParameterAnnotation(Input.class));
				Input methodAnnotation = methodParameter
						.getParameterAnnotation(Input.class);
				inboundName = methodAnnotation.value();
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue,
						"Annotation value must be a String");
				Object targetBean = applicationContext
						.getBean((String) targetReferenceValue);
				BindingProperties bindingProperties = this.bindingServiceProperties
						.getBindingProperties(inboundName);
				enableNativeDecodingForKTableAlways(parameterType, bindingProperties);
				// Retrieve the StreamsConfig created for this method if available.
				// Otherwise, create the StreamsBuilderFactory and get the underlying
				// config.
				if (!this.methodStreamsBuilderFactoryBeanMap.containsKey(method)) {
					buildStreamsBuilderAndRetrieveConfig(method, applicationContext,
							inboundName);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.methodStreamsBuilderFactoryBeanMap
							.get(method);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
							.getExtendedConsumerProperties(inboundName);
					// get state store spec
					KafkaStreamsStateStoreProperties spec = buildStateStoreSpec(method);
					Serde<?> keySerde = this.keyValueSerdeResolver
							.getInboundKeySerde(extendedConsumerProperties);
					Serde<?> valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(
							bindingProperties.getConsumer(), extendedConsumerProperties);

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
						LOG.warn("Detected resetOffsets configured on binding "
								+ inboundName + ". "
								+ "Setting resetOffsets in Kafka Streams binder does not have any effect.");
					}

					if (parameterType.isAssignableFrom(KStream.class)) {
						KStream<?, ?> stream = getkStream(inboundName, spec,
								bindingProperties, streamsBuilder, keySerde, valueSerde,
								autoOffsetReset);
						KStreamBoundElementFactory.KStreamWrapper kStreamWrapper = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
						// wrap the proxy created during the initial target type binding
						// with real object (KStream)
						kStreamWrapper.wrap((KStream<Object, Object>) stream);
						this.kafkaStreamsBindingInformationCatalogue
								.addStreamBuilderFactory(streamsBuilderFactoryBean);
						for (StreamListenerParameterAdapter streamListenerParameterAdapter : adapters) {
							if (streamListenerParameterAdapter.supports(stream.getClass(),
									methodParameter)) {
								arguments[parameterIndex] = streamListenerParameterAdapter
										.adapt(kStreamWrapper, methodParameter);
								break;
							}
						}
						if (arguments[parameterIndex] == null
								&& parameterType.isAssignableFrom(stream.getClass())) {
							arguments[parameterIndex] = stream;
						}
						Assert.notNull(arguments[parameterIndex],
								"Cannot convert argument " + parameterIndex + " of "
										+ method + "from " + stream.getClass() + " to "
										+ parameterType);
					}
					else if (parameterType.isAssignableFrom(KTable.class)) {
						String materializedAs = extendedConsumerProperties
								.getMaterializedAs();
						String bindingDestination = this.bindingServiceProperties
								.getBindingDestination(inboundName);
						KTable<?, ?> table = getKTable(streamsBuilder, keySerde,
								valueSerde, materializedAs, bindingDestination,
								autoOffsetReset);
						KTableBoundElementFactory.KTableWrapper kTableWrapper = (KTableBoundElementFactory.KTableWrapper) targetBean;
						// wrap the proxy created during the initial target type binding
						// with real object (KTable)
						kTableWrapper.wrap((KTable<Object, Object>) table);
						this.kafkaStreamsBindingInformationCatalogue
								.addStreamBuilderFactory(streamsBuilderFactoryBean);
						arguments[parameterIndex] = table;
					}
					else if (parameterType.isAssignableFrom(GlobalKTable.class)) {
						String materializedAs = extendedConsumerProperties
								.getMaterializedAs();
						String bindingDestination = this.bindingServiceProperties
								.getBindingDestination(inboundName);
						GlobalKTable<?, ?> table = getGlobalKTable(streamsBuilder,
								keySerde, valueSerde, materializedAs, bindingDestination,
								autoOffsetReset);
						// @checkstyle:off
						GlobalKTableBoundElementFactory.GlobalKTableWrapper globalKTableWrapper = (GlobalKTableBoundElementFactory.GlobalKTableWrapper) targetBean;
						// @checkstyle:on
						// wrap the proxy created during the initial target type binding
						// with real object (KTable)
						globalKTableWrapper.wrap((GlobalKTable<Object, Object>) table);
						this.kafkaStreamsBindingInformationCatalogue
								.addStreamBuilderFactory(streamsBuilderFactoryBean);
						arguments[parameterIndex] = table;
					}
				}
				catch (Exception ex) {
					throw new IllegalStateException(ex);
				}
			}
			else {
				throw new IllegalStateException(
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

	private GlobalKTable<?, ?> getGlobalKTable(StreamsBuilder streamsBuilder,
											Serde<?> keySerde, Serde<?> valueSerde, String materializedAs,
											String bindingDestination, Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null
				? materializedAsGlobalKTable(streamsBuilder, bindingDestination,
				materializedAs, keySerde, valueSerde, autoOffsetReset)
				: streamsBuilder.globalTable(bindingDestination,
				Consumed.with(keySerde, valueSerde)
						.withOffsetResetPolicy(autoOffsetReset));
	}

	private KTable<?, ?> getKTable(StreamsBuilder streamsBuilder, Serde<?> keySerde,
								Serde<?> valueSerde, String materializedAs, String bindingDestination,
								Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null
				? materializedAs(streamsBuilder, bindingDestination, materializedAs,
				keySerde, valueSerde, autoOffsetReset)
				: streamsBuilder.table(bindingDestination,
				Consumed.with(keySerde, valueSerde)
						.withOffsetResetPolicy(autoOffsetReset));
	}

	private <K, V> KTable<K, V> materializedAs(StreamsBuilder streamsBuilder,
											String destination, String storeName, Serde<K> k, Serde<V> v,
											Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.table(
				this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	private <K, V> GlobalKTable<K, V> materializedAsGlobalKTable(
			StreamsBuilder streamsBuilder, String destination, String storeName,
			Serde<K> k, Serde<V> v, Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.globalTable(
				this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> getMaterialized(
			String storeName, Serde<K> k, Serde<V> v) {
		return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
				.withKeySerde(k).withValueSerde(v);
	}

	private StoreBuilder buildStateStore(KafkaStreamsStateStoreProperties spec) {
		try {

			Serde<?> keySerde = this.keyValueSerdeResolver
					.getStateStoreKeySerde(spec.getKeySerdeString());
			Serde<?> valueSerde = this.keyValueSerdeResolver
					.getStateStoreValueSerde(spec.getValueSerdeString());
			StoreBuilder builder;
			switch (spec.getType()) {
				case KEYVALUE:
					builder = Stores.keyValueStoreBuilder(
							Stores.persistentKeyValueStore(spec.getName()), keySerde,
							valueSerde);
					break;
				case WINDOW:
					builder = Stores
							.windowStoreBuilder(
									Stores.persistentWindowStore(spec.getName(),
											spec.getRetention(), 3, spec.getLength(), false),
									keySerde, valueSerde);
					break;
				case SESSION:
					builder = Stores.sessionStoreBuilder(Stores.persistentSessionStore(
							spec.getName(), spec.getRetention()), keySerde, valueSerde);
					break;
				default:
					throw new UnsupportedOperationException(
							"state store type (" + spec.getType() + ") is not supported!");
			}
			if (spec.isCacheEnabled()) {
				builder = builder.withCachingEnabled();
			}
			if (spec.isLoggingDisabled()) {
				builder = builder.withLoggingDisabled();
			}
			return builder;
		}
		catch (Exception ex) {
			LOG.error("failed to build state store exception : " + ex);
			throw ex;
		}
	}

	private KStream<?, ?> getkStream(String inboundName,
									KafkaStreamsStateStoreProperties storeSpec,
									BindingProperties bindingProperties, StreamsBuilder streamsBuilder,
									Serde<?> keySerde, Serde<?> valueSerde,
									Topology.AutoOffsetReset autoOffsetReset) {
		if (storeSpec != null) {
			StoreBuilder storeBuilder = buildStateStore(storeSpec);
			streamsBuilder.addStateStore(storeBuilder);
			if (LOG.isInfoEnabled()) {
				LOG.info("state store " + storeBuilder.name() + " added to topology");
			}
		}
		String[] bindingTargets = StringUtils.commaDelimitedListToStringArray(
				this.bindingServiceProperties.getBindingDestination(inboundName));

		KStream<?, ?> stream = streamsBuilder.stream(Arrays.asList(bindingTargets),
				Consumed.with(keySerde, valueSerde)
						.withOffsetResetPolicy(autoOffsetReset));
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

	private void enableNativeDecodingForKTableAlways(Class<?> parameterType,
													BindingProperties bindingProperties) {
		if (parameterType.isAssignableFrom(KTable.class)
				|| parameterType.isAssignableFrom(GlobalKTable.class)) {
			if (bindingProperties.getConsumer() == null) {
				bindingProperties.setConsumer(new ConsumerProperties());
			}
			// No framework level message conversion provided for KTable/GlobalKTable, its
			// done by the broker.
			bindingProperties.getConsumer().setUseNativeDecoding(true);
		}
	}

	@SuppressWarnings({"unchecked"})
	private void buildStreamsBuilderAndRetrieveConfig(Method method,
													ApplicationContext applicationContext, String inboundName) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext
				.getBeanFactory();

		Map<String, Object> streamConfigGlobalProperties = applicationContext
				.getBean("streamConfigGlobalProperties", Map.class);

		KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName);
		streamConfigGlobalProperties
				.putAll(extendedConsumerProperties.getConfiguration());

		String applicationId = extendedConsumerProperties.getApplicationId();
		// override application.id if set at the individual binding level.
		if (StringUtils.hasText(applicationId)) {
			streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
					applicationId);
		}

		int concurrency = this.bindingServiceProperties.getConsumerProperties(inboundName)
				.getConcurrency();
		// override concurrency if set at the individual binding level.
		if (concurrency > 1) {
			streamConfigGlobalProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
					concurrency);
		}

		Map<String, KafkaStreamsDlqDispatch> kafkaStreamsDlqDispatchers = applicationContext
				.getBean("kafkaStreamsDlqDispatchers", Map.class);

		KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(
				streamConfigGlobalProperties) {
			@Override
			public Properties asProperties() {
				Properties properties = super.asProperties();
				properties.put(SendToDlqAndContinue.KAFKA_STREAMS_DLQ_DISPATCHERS,
						kafkaStreamsDlqDispatchers);
				return properties;
			}
		};

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
				"stream-builder-" + method.getName(), streamsBuilderBeanDefinition);
		StreamsBuilderFactoryBean streamsBuilderX = applicationContext.getBean(
				"&stream-builder-" + method.getName(), StreamsBuilderFactoryBean.class);
		this.methodStreamsBuilderFactoryBeanMap.put(method, streamsBuilderX);
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	private void validateStreamListenerMethod(StreamListener streamListener,
											Method method, String[] methodAnnotatedOutboundNames) {
		String methodAnnotatedInboundName = streamListener.value();
		if (methodAnnotatedOutboundNames != null) {
			for (String s : methodAnnotatedOutboundNames) {
				if (StringUtils.hasText(s)) {
					Assert.isTrue(isDeclarativeOutput(method, s),
							"Method must be declarative");
				}
			}
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			int methodArgumentsLength = method.getParameterTypes().length;

			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method,
						parameterIndex);
				Assert.isTrue(
						isDeclarativeInput(methodAnnotatedInboundName, methodParameter),
						"Method must be declarative");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeOutput(Method m, String targetBeanName) {
		boolean declarative;
		Class<?> returnType = m.getReturnType();
		if (returnType.isArray()) {
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			declarative = this.streamListenerResultAdapters.stream()
					.anyMatch((slpa) -> slpa.supports(returnType.getComponentType(),
							targetBeanClass));
			return declarative;
		}
		Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
		declarative = this.streamListenerResultAdapters.stream()
				.anyMatch((slpa) -> slpa.supports(returnType, targetBeanClass));
		return declarative;
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeInput(String targetBeanName,
									MethodParameter methodParameter) {
		if (!methodParameter.getParameterType().isAssignableFrom(Object.class)
				&& this.applicationContext.containsBean(targetBeanName)) {
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			if (targetBeanClass != null) {
				boolean supports = KStream.class.isAssignableFrom(targetBeanClass)
						&& KStream.class.isAssignableFrom(methodParameter.getParameterType());
				if (!supports) {
					supports = KTable.class.isAssignableFrom(targetBeanClass)
							&& KTable.class.isAssignableFrom(methodParameter.getParameterType());
					if (!supports) {
						supports = GlobalKTable.class.isAssignableFrom(targetBeanClass)
								&& GlobalKTable.class.isAssignableFrom(methodParameter.getParameterType());
					}
				}
				return supports;
			}
		}
		return false;
	}

	private static String[] getOutboundBindingTargetNames(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()),
					StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length >= 1,
					"At least one outbound destination need to be provided.");
			return sendTo.value();
		}
		return null;
	}

	@SuppressWarnings({"unchecked"})
	private KafkaStreamsStateStoreProperties buildStateStoreSpec(Method method) {
		if (!this.registeredStoresPerMethod.containsKey(method)) {
			KafkaStreamsStateStore spec = AnnotationUtils.findAnnotation(method,
					KafkaStreamsStateStore.class);
			if (spec != null) {
				Assert.isTrue(!ObjectUtils.isEmpty(spec.name()), "name cannot be empty");
				Assert.isTrue(spec.name().length() >= 1, "name cannot be empty.");
				this.registeredStoresPerMethod.put(method, new ArrayList<>());
				this.registeredStoresPerMethod.get(method).add(spec.name());
				KafkaStreamsStateStoreProperties props = new KafkaStreamsStateStoreProperties();
				props.setName(spec.name());
				props.setType(spec.type());
				props.setLength(spec.lengthMs());
				props.setKeySerdeString(spec.keySerde());
				props.setRetention(spec.retentionMs());
				props.setValueSerdeString(spec.valueSerde());
				props.setCacheEnabled(spec.cache());
				props.setLoggingDisabled(!spec.logging());
				return props;
			}
		}
		return null;
	}

}
