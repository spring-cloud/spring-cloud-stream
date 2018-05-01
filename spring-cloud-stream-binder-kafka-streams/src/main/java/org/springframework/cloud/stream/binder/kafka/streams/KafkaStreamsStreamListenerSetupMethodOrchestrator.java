/*
 * Copyright 2018 the original author or authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
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
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
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
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.messaging.Message;
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
 *
 * The orchestration primarily focus on the following areas:
 *
 * 1. Allow multiple KStream output bindings (KStream branching) by allowing more than one output values on {@link SendTo}
 * 2. Allow multiple inbound bindings for multiple KStream and or KTable types.
 * 3. Each StreamListener method that it orchestrates gets its own {@link StreamsBuilderFactoryBean} and {@link StreamsConfig}
 *
 * @author Soby Chacko
 * @author Lei Chen
 */
class KafkaStreamsStreamListenerSetupMethodOrchestrator implements StreamListenerSetupMethodOrchestrator, ApplicationContextAware {

	private final static Log LOG = LogFactory.getLog(KafkaStreamsStreamListenerSetupMethodOrchestrator.class);

	private final StreamListenerParameterAdapter streamListenerParameterAdapter;

	private final Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

	private final BindingServiceProperties bindingServiceProperties;

	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final Map<Method, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private ConfigurableApplicationContext applicationContext;

	KafkaStreamsStreamListenerSetupMethodOrchestrator(BindingServiceProperties bindingServiceProperties,
														KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
														KeyValueSerdeResolver keyValueSerdeResolver,
														KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
														StreamListenerParameterAdapter streamListenerParameterAdapter,
														Collection<StreamListenerResultAdapter> streamListenerResultAdapters,
														KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.streamListenerParameterAdapter = streamListenerParameterAdapter;
		this.streamListenerResultAdapters = streamListenerResultAdapters;
		this.binderConfigurationProperties = binderConfigurationProperties;
	}

	@Override
	public boolean supports(Method method) {
		return methodParameterSupports(method) &&
				(methodReturnTypeSuppports(method) || Void.TYPE.equals(method.getReturnType()));
	}

	private boolean methodReturnTypeSuppports(Method method) {
		Class<?> returnType = method.getReturnType();
		if (returnType.equals(KStream.class) ||
				(returnType.isArray() && returnType.getComponentType().equals(KStream.class))) {
			return true;
		}
		return false;
	}

	private boolean methodParameterSupports(Method method) {
		boolean supports = false;
		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, i);
			Class<?> parameterType = methodParameter.getParameterType();
			if (parameterType.equals(KStream.class) || parameterType.equals(KTable.class)) {
				supports = true;
			}
		}
		return supports;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void orchestrateStreamListenerSetupMethod(StreamListener streamListener, Method method, Object bean) {
		String[] methodAnnotatedOutboundNames = getOutboundBindingTargetNames(method);
		validateStreamListenerMethod(streamListener, method, methodAnnotatedOutboundNames);
		String methodAnnotatedInboundName = streamListener.value();
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(method, methodAnnotatedInboundName,
				this.applicationContext,
				this.streamListenerParameterAdapter);
		try {
			ReflectionUtils.makeAccessible(method);
			if (Void.TYPE.equals(method.getReturnType())) {
				method.invoke(bean, adaptedInboundArguments);
			}
			else {
				Object result = method.invoke(bean, adaptedInboundArguments);

				if (result.getClass().isArray()) {
					Assert.isTrue(methodAnnotatedOutboundNames.length == ((Object[]) result).length,
							"Result does not match with the number of declared outbounds");
				} else {
					Assert.isTrue(methodAnnotatedOutboundNames.length == 1,
							"Result does not match with the number of declared outbounds");
				}
				if (result.getClass().isArray()) {
					Object[] outboundKStreams = (Object[]) result;
					int i = 0;
					for (Object outboundKStream : outboundKStreams) {
						Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[i++]);
						for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
							if (streamListenerResultAdapter.supports(outboundKStream.getClass(), targetBean.getClass())) {
								streamListenerResultAdapter.adapt(outboundKStream, targetBean);
								break;
							}
						}
					}
				} else {
					Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[0]);
					for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
						if (streamListenerResultAdapter.supports(result.getClass(), targetBean.getClass())) {
							streamListenerResultAdapter.adapt(result, targetBean);
							break;
						}
					}
				}
			}
		}
		catch (Exception e) {
			throw new BeanInitializationException("Cannot setup StreamListener for " + method, e);
		}
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public Object[] adaptAndRetrieveInboundArguments(Method method, String inboundName,
													ApplicationContext applicationContext,
													StreamListenerParameterAdapter... streamListenerParameterAdapters) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
				Input methodAnnotation = methodParameter.getParameterAnnotation(Input.class);
				inboundName = methodAnnotation.value();
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue, "Annotation value must be a String");
				Object targetBean = applicationContext.getBean((String) targetReferenceValue);
				BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(inboundName);
				enableNativeDecodingForKTableAlways(parameterType, bindingProperties);
				StreamsConfig streamsConfig = null;
				//Retrieve the StreamsConfig created for this method if available.
				//Otherwise, create the StreamsBuilderFactory and get the underlying config.
				if (!methodStreamsBuilderFactoryBeanMap.containsKey(method)) {
					streamsConfig = buildStreamsBuilderAndRetrieveConfig(method, applicationContext, bindingProperties);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = methodStreamsBuilderFactoryBeanMap.get(method);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					KafkaStreamsConsumerProperties extendedConsumerProperties = kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties(inboundName);
					//get state store spec
					KafkaStreamsStateStoreProperties spec = buildStateStoreSpec(method);
					Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties);
					Serde<?> valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(bindingProperties.getConsumer(), extendedConsumerProperties);
					if (parameterType.isAssignableFrom(KStream.class)) {
						KStream<?, ?> stream = getkStream(inboundName, spec, bindingProperties, streamsBuilder, keySerde, valueSerde);
						KStreamBoundElementFactory.KStreamWrapper kStreamWrapper = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KStream)
						kStreamWrapper.wrap((KStream<Object, Object>) stream);
						kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
						if (streamsConfig != null){
							kafkaStreamsBindingInformationCatalogue.addStreamsConfigs(kStreamWrapper, streamsConfig);
						}
						for (StreamListenerParameterAdapter streamListenerParameterAdapter : streamListenerParameterAdapters) {
							if (streamListenerParameterAdapter.supports(stream.getClass(), methodParameter)) {
								arguments[parameterIndex] = streamListenerParameterAdapter.adapt(kStreamWrapper, methodParameter);
								break;
							}
						}
						if (arguments[parameterIndex] == null && parameterType.isAssignableFrom(stream.getClass())) {
							arguments[parameterIndex] = stream;
						}
						Assert.notNull(arguments[parameterIndex], "Cannot convert argument " + parameterIndex + " of " + method
								+ "from " + stream.getClass() + " to " + parameterType);
					}
					else if (parameterType.isAssignableFrom(KTable.class)) {
						String materializedAs = extendedConsumerProperties.getMaterializedAs();
						String bindingDestination = bindingServiceProperties.getBindingDestination(inboundName);
						KTable<?, ?> table = materializedAs != null ?
								materializedAs(streamsBuilder, bindingDestination, materializedAs, keySerde, valueSerde ) :
								streamsBuilder.table(bindingDestination,
										Consumed.with(keySerde, valueSerde));
						KTableBoundElementFactory.KTableWrapper kTableWrapper = (KTableBoundElementFactory.KTableWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KTable)
						kTableWrapper.wrap((KTable<Object, Object>) table);
						kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
						if (streamsConfig != null){
							kafkaStreamsBindingInformationCatalogue.addStreamsConfigs(kTableWrapper, streamsConfig);
						}
						arguments[parameterIndex] = table;
					}
				}
				catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
			else {
				throw new IllegalStateException(StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

	private <K,V> KTable<K,V> materializedAs(StreamsBuilder streamsBuilder, String destination, String storeName, Serde<K> k, Serde<V> v) {
		return streamsBuilder.table(bindingServiceProperties.getBindingDestination(destination),
				Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
						.withKeySerde(k)
						.withValueSerde(v));
	}

	private StoreBuilder buildStateStore(KafkaStreamsStateStoreProperties spec) {
		try {
			Serde<?> keySerde = this.keyValueSerdeResolver.getStateStoreKeySerde(spec.getKeySerdeString());
			Serde<?> valueSerde = this.keyValueSerdeResolver.getStateStoreValueSerde(spec.getValueSerdeString());
			StoreBuilder builder;
			switch (spec.getType()) {
				case KEYVALUE:
					builder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(spec.getName()), keySerde, valueSerde);
					break;
				case WINDOW:
					builder = Stores.windowStoreBuilder(Stores.persistentWindowStore(spec.getName(), spec.getRetention(), 3, spec.getLength(), false),
							keySerde,
							valueSerde);
					break;
				case SESSION:
					builder = Stores.sessionStoreBuilder(Stores.persistentSessionStore(spec.getName(), spec.getRetention()), keySerde, valueSerde);
					break;
				default:
					throw new UnsupportedOperationException("state store type (" + spec.getType() + ") is not supported!");
			}
			if (spec.isCacheEnabled()) {
				builder = builder.withCachingEnabled();
			}
			if (spec.isLoggingDisabled()) {
				builder = builder.withLoggingDisabled();
			}

			return builder;

		}catch (Exception e) {
			LOG.error("failed to build state store exception : " + e);
			throw e;
		}
	}


	private KStream<?, ?> getkStream(String inboundName, KafkaStreamsStateStoreProperties storeSpec,
									BindingProperties bindingProperties, StreamsBuilder streamsBuilder,
									Serde<?> keySerde, Serde<?> valueSerde) {
		if (storeSpec != null) {
			StoreBuilder storeBuilder = buildStateStore(storeSpec);
			streamsBuilder.addStateStore(storeBuilder);
			if (LOG.isInfoEnabled()) {
				LOG.info("state store " + storeBuilder.name() + " added to topology");
			}
		}
		String[] bindingTargets = StringUtils
				.commaDelimitedListToStringArray(bindingServiceProperties.getBindingDestination(inboundName));
		KStream<?, ?> stream = streamsBuilder.stream(Arrays.asList(bindingTargets),
				Consumed.with(keySerde, valueSerde));
		final boolean nativeDecoding = bindingServiceProperties.getConsumerProperties(inboundName).isUseNativeDecoding();
		if (nativeDecoding){
			LOG.info("Native decoding is enabled for " + inboundName + ". Inbound deserialization done at the broker.");
		}
		else {
			LOG.info("Native decoding is disabled for " + inboundName + ". Inbound message conversion done by Spring Cloud Stream.");
		}

		stream = stream.mapValues(value -> {
			Object returnValue;
			String contentType = bindingProperties.getContentType();
			if (!StringUtils.isEmpty(contentType) && !nativeDecoding) {
				Message<?> message = MessageBuilder.withPayload(value)
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
						returnValue = message;
			} else {
				returnValue = value;
			}
			return returnValue;
		});
		return stream;
	}

	private void enableNativeDecodingForKTableAlways(Class<?> parameterType, BindingProperties bindingProperties) {
		if (parameterType.isAssignableFrom(KTable.class)) {
			if (bindingProperties.getConsumer() == null) {
				bindingProperties.setConsumer(new ConsumerProperties());
			}
			//No framework level message conversion provided for KTable, its done by the broker.
			bindingProperties.getConsumer().setUseNativeDecoding(true);
		}
	}

	@SuppressWarnings({"unchecked"})
	private StreamsConfig buildStreamsBuilderAndRetrieveConfig(Method method, ApplicationContext applicationContext,
															BindingProperties bindingProperties) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext.getBeanFactory();
		StreamsBuilderFactoryBean streamsBuilder = new StreamsBuilderFactoryBean();
		streamsBuilder.setAutoStartup(false);
		String uuid = UUID.randomUUID().toString();
		BeanDefinition streamsBuilderBeanDefinition =
				BeanDefinitionBuilder.genericBeanDefinition((Class<StreamsBuilderFactoryBean>) streamsBuilder.getClass(), () -> streamsBuilder)
				.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("stream-builder-" + uuid, streamsBuilderBeanDefinition);
		StreamsBuilderFactoryBean streamsBuilderX = applicationContext.getBean("&stream-builder-" + uuid, StreamsBuilderFactoryBean.class);
		String group = bindingProperties.getGroup();
		if (!StringUtils.hasText(group)) {
			group = binderConfigurationProperties.getApplicationId();
		}
		Map<String, Object> streamConfigGlobalProperties = applicationContext.getBean("streamConfigGlobalProperties", Map.class);
		streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, group);

		//Custom StreamsConfig implementation that overrides to guarantee that the deserialization handler is cached.
		StreamsConfig streamsConfig = new StreamsConfig(streamConfigGlobalProperties) {
			DeserializationExceptionHandler deserializationExceptionHandler;
			@Override
			@SuppressWarnings("unchecked")
			public <T> T getConfiguredInstance(String key, Class<T> clazz) {
				if (key.equals(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)){
					if (deserializationExceptionHandler != null){
						return (T)deserializationExceptionHandler;
					}
					else {
						T t = super.getConfiguredInstance(key, clazz);
						deserializationExceptionHandler = (DeserializationExceptionHandler)t;
						return t;
					}
				}
				return super.getConfiguredInstance(key, clazz);
			}
		};
		BeanDefinition streamsConfigBeanDefinition =
				BeanDefinitionBuilder.genericBeanDefinition((Class<StreamsConfig>) streamsConfig.getClass(), () -> streamsConfig)
						.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("streamsConfig-" + uuid, streamsConfigBeanDefinition);

		streamsBuilder.setStreamsConfig(streamsConfig);
		methodStreamsBuilderFactoryBeanMap.put(method, streamsBuilderX);
		return streamsConfig;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	private void validateStreamListenerMethod(StreamListener streamListener, Method method, String[] methodAnnotatedOutboundNames) {
		String methodAnnotatedInboundName = streamListener.value();
		if (methodAnnotatedOutboundNames != null) {
			for (String s : methodAnnotatedOutboundNames) {
				if (StringUtils.hasText(s)) {
					Assert.isTrue(isDeclarativeOutput(method, s), "Method must be declarative");
				}
			}
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			int methodArgumentsLength = method.getParameterTypes().length;

			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
				Assert.isTrue(isDeclarativeInput(methodAnnotatedInboundName, methodParameter), "Method must be declarative");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeOutput(Method m, String targetBeanName) {
		boolean declarative;
		Class<?> returnType = m.getReturnType();
		if (returnType.isArray()){
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			declarative = this.streamListenerResultAdapters.stream()
					.anyMatch(slpa -> slpa.supports(returnType.getComponentType(), targetBeanClass));
			return declarative;
		}
		Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
		declarative = this.streamListenerResultAdapters.stream()
				.anyMatch(slpa -> slpa.supports(returnType, targetBeanClass));
		return declarative;
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeInput(String targetBeanName, MethodParameter methodParameter) {
		if (!methodParameter.getParameterType().isAssignableFrom(Object.class) && this.applicationContext.containsBean(targetBeanName)) {
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			return this.streamListenerParameterAdapter.supports(targetBeanClass, methodParameter);
		}
		return false;
	}

	private static String[] getOutboundBindingTargetNames(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length >= 1, "At least one outbound destination need to be provided.");
			return sendTo.value();
		}
		return null;
	}

	@SuppressWarnings({"unchecked"})
	private static KafkaStreamsStateStoreProperties buildStateStoreSpec(Method method) {
		KafkaStreamsStateStore spec = AnnotationUtils.findAnnotation(method, KafkaStreamsStateStore.class);
		if (spec != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(spec.name()), "name cannot be empty");
			Assert.isTrue(spec.name().length() >= 1, "name cannot be empty.");
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
		return null;
	}
}
