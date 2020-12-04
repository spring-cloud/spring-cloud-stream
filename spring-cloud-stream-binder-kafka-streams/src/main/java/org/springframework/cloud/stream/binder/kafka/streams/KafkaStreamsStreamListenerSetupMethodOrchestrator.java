/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
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
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.messaging.handler.annotation.SendTo;
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
class KafkaStreamsStreamListenerSetupMethodOrchestrator extends AbstractKafkaStreamsBinderProcessor
		implements StreamListenerSetupMethodOrchestrator {

	private static final Log LOG = LogFactory
			.getLog(KafkaStreamsStreamListenerSetupMethodOrchestrator.class);

	private final StreamListenerParameterAdapter streamListenerParameterAdapter;

	private final Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

	private final BindingServiceProperties bindingServiceProperties;

	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final Map<Method, List<String>> registeredStoresPerMethod = new HashMap<>();

	private final Map<Method, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();

	StreamsBuilderFactoryBeanCustomizer customizer;

	private final ConfigurableEnvironment environment;

	KafkaStreamsStreamListenerSetupMethodOrchestrator(
			BindingServiceProperties bindingServiceProperties,
			KafkaStreamsExtendedBindingProperties extendedBindingProperties,
			KeyValueSerdeResolver keyValueSerdeResolver,
			KafkaStreamsBindingInformationCatalogue bindingInformationCatalogue,
			StreamListenerParameterAdapter streamListenerParameterAdapter,
			Collection<StreamListenerResultAdapter> listenerResultAdapters,
			CleanupConfig cleanupConfig,
			StreamsBuilderFactoryBeanCustomizer customizer,
			ConfigurableEnvironment environment) {
		super(bindingServiceProperties, bindingInformationCatalogue, extendedBindingProperties, keyValueSerdeResolver, cleanupConfig);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = extendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = bindingInformationCatalogue;
		this.streamListenerParameterAdapter = streamListenerParameterAdapter;
		this.streamListenerResultAdapters = listenerResultAdapters;
		this.customizer = customizer;
		this.environment = environment;
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

				if (methodAnnotatedOutboundNames != null && methodAnnotatedOutboundNames.length > 0) {
					if (result.getClass().isArray()) {
						Assert.isTrue(
								methodAnnotatedOutboundNames.length == ((Object[]) result).length,
								"Result does not match with the number of declared outbounds");
					}
					else {
						Assert.isTrue(methodAnnotatedOutboundNames.length == 1,
								"Result does not match with the number of declared outbounds");
					}
				}

				if (methodAnnotatedOutboundNames != null && methodAnnotatedOutboundNames.length > 0) {
					if (result.getClass().isArray()) {
						Object[] outboundKStreams = (Object[]) result;
						int i = 0;
						for (Object outboundKStream : outboundKStreams) {
							Object targetBean = this.applicationContext
									.getBean(methodAnnotatedOutboundNames[i++]);
							kafkaStreamsBindingInformationCatalogue.addOutboundKStreamResolvable(targetBean, ResolvableType.forMethodReturnType(method));
							adaptStreamListenerResult(outboundKStream, targetBean);
						}
					}
					else {
						Object targetBean = this.applicationContext
								.getBean(methodAnnotatedOutboundNames[0]);
						kafkaStreamsBindingInformationCatalogue.addOutboundKStreamResolvable(targetBean, ResolvableType.forMethodReturnType(method));
						adaptStreamListenerResult(result, targetBean);
					}
				}
			}
		}
		catch (Exception ex) {
			throw new BeanInitializationException(
					"Cannot setup StreamListener for " + method, ex);
		}
	}

	@SuppressWarnings("unchecked")
	private void adaptStreamListenerResult(Object outboundKStream, Object targetBean) {
		for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
			if (streamListenerResultAdapter.supports(
					outboundKStream.getClass(), targetBean.getClass())) {
				streamListenerResultAdapter.adapt(outboundKStream,
						targetBean);
				break;
			}
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
				// Retrieve the StreamsConfig created for this method if available.
				// Otherwise, create the StreamsBuilderFactory and get the underlying
				// config.
				if (!this.methodStreamsBuilderFactoryBeanMap.containsKey(method)) {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = buildStreamsBuilderAndRetrieveConfig(method.getDeclaringClass().getSimpleName() + "-" + method.getName(),
							applicationContext,
							inboundName, null, customizer, this.environment, bindingProperties);
					this.methodStreamsBuilderFactoryBeanMap.put(method, streamsBuilderFactoryBean);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.methodStreamsBuilderFactoryBeanMap
							.get(method);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					final String applicationId = streamsBuilderFactoryBean.getStreamsConfiguration().getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
					KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
							.getExtendedConsumerProperties(inboundName);
					extendedConsumerProperties.setApplicationId(applicationId);
					// get state store spec
					KafkaStreamsStateStoreProperties spec = buildStateStoreSpec(method);

					Serde<?> keySerde = this.keyValueSerdeResolver
							.getInboundKeySerde(extendedConsumerProperties, ResolvableType.forMethodParameter(methodParameter));
					LOG.info("Key Serde used for " + targetReferenceValue + ": " + keySerde.getClass().getName());

					Serde<?> valueSerde = bindingServiceProperties.getConsumerProperties(inboundName).isUseNativeDecoding() ?
						getValueSerde(inboundName, extendedConsumerProperties, ResolvableType.forMethodParameter(methodParameter)) : Serdes.ByteArray();
					LOG.info("Value Serde used for " + targetReferenceValue + ": " + valueSerde.getClass().getName());

					Topology.AutoOffsetReset autoOffsetReset = getAutoOffsetReset(inboundName, extendedConsumerProperties);

					if (parameterType.isAssignableFrom(KStream.class)) {
						KStream<?, ?> stream = getkStream(inboundName, spec,
								bindingProperties, extendedConsumerProperties, streamsBuilder, keySerde, valueSerde,
								autoOffsetReset, parameterIndex == 0);
						KStreamBoundElementFactory.KStreamWrapper kStreamWrapper = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
						// wrap the proxy created during the initial target type binding
						// with real object (KStream)
						kStreamWrapper.wrap((KStream<Object, Object>) stream);
						this.kafkaStreamsBindingInformationCatalogue.addKeySerde(stream, keySerde);
						BindingProperties bindingProperties1 = this.kafkaStreamsBindingInformationCatalogue.getBindingProperties().get(kStreamWrapper);
						this.kafkaStreamsBindingInformationCatalogue.registerBindingProperties(stream, bindingProperties1);

						this.kafkaStreamsBindingInformationCatalogue
								.addStreamBuilderFactory(streamsBuilderFactoryBean);
						for (StreamListenerParameterAdapter streamListenerParameterAdapter : adapters) {
							if (streamListenerParameterAdapter.supports(stream.getClass(),
									methodParameter)) {
								arguments[parameterIndex] = streamListenerParameterAdapter
										.adapt(stream, methodParameter);
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
					else {
						handleKTableGlobalKTableInputs(arguments, parameterIndex, inboundName, parameterType, targetBean, streamsBuilderFactoryBean,
								streamsBuilder, extendedConsumerProperties, keySerde, valueSerde, autoOffsetReset, parameterIndex == 0);
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
									BindingProperties bindingProperties,
									KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties, StreamsBuilder streamsBuilder,
									Serde<?> keySerde, Serde<?> valueSerde,
									Topology.AutoOffsetReset autoOffsetReset, boolean firstBuild) {
		if (storeSpec != null) {
			StoreBuilder storeBuilder = buildStateStore(storeSpec);
			streamsBuilder.addStateStore(storeBuilder);
			if (LOG.isInfoEnabled()) {
				LOG.info("state store " + storeBuilder.name() + " added to topology");
			}
		}
		return getKStream(inboundName, bindingProperties, kafkaStreamsConsumerProperties, streamsBuilder,
				keySerde, valueSerde, autoOffsetReset, firstBuild);
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
				boolean supports = KafkaStreamsBinderUtils.supportsKStream(methodParameter, targetBeanClass);
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
