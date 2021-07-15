/*
 * Copyright 2019-2021 the original author or authors.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.binder.kafka.streams.function.KafkaStreamsBindableProxyFactory;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.FunctionConstants;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
public class KafkaStreamsFunctionProcessor extends AbstractKafkaStreamsBinderProcessor implements BeanFactoryAware {

	private static final Log LOG = LogFactory.getLog(KafkaStreamsFunctionProcessor.class);
	private static final String OUTBOUND = "outbound";

	private final BindingServiceProperties bindingServiceProperties;
	private final Map<String, StreamsBuilderFactoryBean> methodStreamsBuilderFactoryBeanMap = new HashMap<>();
	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;
	private final KeyValueSerdeResolver keyValueSerdeResolver;
	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;
	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;

	private BeanFactory beanFactory;
	private StreamFunctionProperties streamFunctionProperties;
	private KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties;
	StreamsBuilderFactoryBeanConfigurer customizer;
	ConfigurableEnvironment environment;

	public KafkaStreamsFunctionProcessor(BindingServiceProperties bindingServiceProperties,
										KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
										KeyValueSerdeResolver keyValueSerdeResolver,
										KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
										KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
										CleanupConfig cleanupConfig,
										StreamFunctionProperties streamFunctionProperties,
										KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
										StreamsBuilderFactoryBeanConfigurer customizer, ConfigurableEnvironment environment) {
		super(bindingServiceProperties, kafkaStreamsBindingInformationCatalogue, kafkaStreamsExtendedBindingProperties,
				keyValueSerdeResolver, cleanupConfig);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.streamFunctionProperties = streamFunctionProperties;
		this.kafkaStreamsBinderConfigurationProperties = kafkaStreamsBinderConfigurationProperties;
		this.customizer = customizer;
		this.environment = environment;
	}

	private Map<String, ResolvableType> buildTypeMap(ResolvableType resolvableType,
													KafkaStreamsBindableProxyFactory kafkaStreamsBindableProxyFactory,
													Method method, String functionName) {
		Map<String, ResolvableType> resolvableTypeMap = new LinkedHashMap<>();
		if (method != null) { // Component functional bean.
			final ResolvableType firstMethodParameter = ResolvableType.forMethodParameter(method, 0);
			ResolvableType currentOutputGeneric = ResolvableType.forMethodReturnType(method);

			final Set<String> inputs = new LinkedHashSet<>(kafkaStreamsBindableProxyFactory.getInputs());
			final Iterator<String> iterator = inputs.iterator();
			populateResolvableTypeMap(firstMethodParameter, resolvableTypeMap, iterator, method, functionName);

			final Class<?> outputRawclass = currentOutputGeneric.getRawClass();
			traverseReturnTypeForComponentBeans(resolvableTypeMap, currentOutputGeneric, inputs, iterator, outputRawclass);
		}
		else if (resolvableType != null && resolvableType.getRawClass() != null) {
			int inputCount = 1;

			ResolvableType currentOutputGeneric;
			if (resolvableType.getRawClass().isAssignableFrom(BiFunction.class) ||
					resolvableType.getRawClass().isAssignableFrom(BiConsumer.class)) {
				inputCount = 2;
				currentOutputGeneric = resolvableType.getGeneric(2);
			}
			else {
				currentOutputGeneric = resolvableType.getGeneric(1);
			}
			while (currentOutputGeneric.getRawClass() != null && functionOrConsumerFound(currentOutputGeneric)) {
				inputCount++;
				currentOutputGeneric = currentOutputGeneric.getGeneric(1);
			}
			final Set<String> inputs = new LinkedHashSet<>(kafkaStreamsBindableProxyFactory.getInputs());

			final Iterator<String> iterator = inputs.iterator();

			populateResolvableTypeMap(resolvableType, resolvableTypeMap, iterator);

			ResolvableType iterableResType = resolvableType;
			int i = resolvableType.getRawClass().isAssignableFrom(BiFunction.class) ||
					resolvableType.getRawClass().isAssignableFrom(BiConsumer.class) ? 2 : 1;
			ResolvableType outboundResolvableType;
			if (i == inputCount) {
				outboundResolvableType = iterableResType.getGeneric(i);
			}
			else {
				while (i < inputCount && iterator.hasNext()) {
					iterableResType = iterableResType.getGeneric(1);
					if (iterableResType.getRawClass() != null &&
							functionOrConsumerFound(iterableResType)) {
						populateResolvableTypeMap(iterableResType, resolvableTypeMap, iterator);
					}
					i++;
				}
				outboundResolvableType = iterableResType.getGeneric(1);
			}
			resolvableTypeMap.put(OUTBOUND, outboundResolvableType);
		}
		return resolvableTypeMap;
	}

	private void traverseReturnTypeForComponentBeans(Map<String, ResolvableType> resolvableTypeMap, ResolvableType currentOutputGeneric,
													Set<String> inputs, Iterator<String> iterator, Class<?> outputRawclass) {
		if (outputRawclass != null && !outputRawclass.equals(Void.TYPE)) {
			ResolvableType iterableResType = currentOutputGeneric;
			int i = 1;
			// Traverse through the return signature.
			while (i < inputs.size() && iterator.hasNext()) {
				if (iterableResType.getRawClass() != null &&
						functionOrConsumerFound(iterableResType)) {
					populateResolvableTypeMap(iterableResType, resolvableTypeMap, iterator);
				}
				iterableResType = iterableResType.getGeneric(1);
				i++;
			}
			if (iterableResType.getRawClass() != null && KStream.class.isAssignableFrom(iterableResType.getRawClass())) {
				resolvableTypeMap.put(OUTBOUND, iterableResType);
			}
		}
	}

	private boolean functionOrConsumerFound(ResolvableType iterableResType) {
		return iterableResType.getRawClass().equals(Function.class) ||
				iterableResType.getRawClass().equals(Consumer.class);
	}

	private void populateResolvableTypeMap(ResolvableType resolvableType, Map<String, ResolvableType> resolvableTypeMap,
										Iterator<String> iterator) {
		final String next = iterator.next();
		resolvableTypeMap.put(next, resolvableType.getGeneric(0));
		if (resolvableType.getRawClass() != null &&
				(resolvableType.getRawClass().isAssignableFrom(BiFunction.class) ||
				resolvableType.getRawClass().isAssignableFrom(BiConsumer.class))
			&& iterator.hasNext()) {
			resolvableTypeMap.put(iterator.next(), resolvableType.getGeneric(1));
		}
	}

	private void populateResolvableTypeMap(ResolvableType resolvableType, Map<String, ResolvableType> resolvableTypeMap,
										Iterator<String> iterator, Method method, String functionName) {
		final String next = iterator.next();
		resolvableTypeMap.put(next, resolvableType);
		if (method != null) {
			final Object bean = beanFactory.getBean(functionName);
			if (BiFunction.class.isAssignableFrom(bean.getClass()) || BiConsumer.class.isAssignableFrom(bean.getClass())) {
				resolvableTypeMap.put(iterator.next(), ResolvableType.forMethodParameter(method, 1));
			}
		}
	}

	private ResolvableType checkOutboundForComposedFunctions(
			ResolvableType outputResolvableType) {

		ResolvableType currentOutputGeneric;

		if (outputResolvableType.getRawClass() != null && outputResolvableType.getRawClass().isAssignableFrom(BiFunction.class)) {
			currentOutputGeneric = outputResolvableType.getGeneric(2);
		}
		else {
			currentOutputGeneric = outputResolvableType.getGeneric(1);
		}
		while (currentOutputGeneric.getRawClass() != null && functionOrConsumerFound(currentOutputGeneric)) {
			currentOutputGeneric = currentOutputGeneric.getGeneric(1);
		}
		return currentOutputGeneric;
	}

	/**
	 * This method must be kept stateless. In the case of multiple function beans in an application,
	 * isolated {@link KafkaStreamsBindableProxyFactory} instances are passed in separately for those functions. If the
	 * state is shared between invocations, that will create potential race conditions. Hence, invocations of this method
	 * should not be dependent on state modified by a previous invocation.
	 *
	 * @param resolvableType type of the binding
	 * @param functionName bean name of the function
	 * @param kafkaStreamsBindableProxyFactory bindable proxy factory for the Kafka Streams type
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setupFunctionInvokerForKafkaStreams(ResolvableType resolvableType, String functionName,
													KafkaStreamsBindableProxyFactory kafkaStreamsBindableProxyFactory, Method method,
													ResolvableType outputResolvableType,
													String... composedFunctionNames) {
		final Map<String, ResolvableType> resolvableTypes = buildTypeMap(resolvableType,
				kafkaStreamsBindableProxyFactory, method, functionName);

		ResolvableType outboundResolvableType;
		if (outputResolvableType != null) {
			outboundResolvableType = checkOutboundForComposedFunctions(outputResolvableType);
			resolvableTypes.remove(OUTBOUND);
		}
		else {
			outboundResolvableType = resolvableTypes.remove(OUTBOUND);
		}

		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(resolvableTypes, functionName);
		try {
			if (resolvableType.getRawClass() != null && resolvableType.getRawClass().equals(Consumer.class)) {
				Consumer<Object> consumer = (Consumer) this.beanFactory.getBean(functionName);
				consumer.accept(adaptedInboundArguments[0]);
			}
			else if (resolvableType.getRawClass() != null && resolvableType.getRawClass().equals(BiConsumer.class)) {
				BiConsumer<Object, Object> biConsumer = (BiConsumer) this.beanFactory.getBean(functionName);
				biConsumer.accept(adaptedInboundArguments[0], adaptedInboundArguments[1]);
			}
			else if (method != null) { // Handling component functional beans
				final Object bean = beanFactory.getBean(functionName);
				if (Consumer.class.isAssignableFrom(bean.getClass())) {
					((Consumer) bean).accept(adaptedInboundArguments[0]);
				}
				else if (BiConsumer.class.isAssignableFrom(bean.getClass())) {
					((BiConsumer) bean).accept(adaptedInboundArguments[0], adaptedInboundArguments[1]);
				}
				else if (Function.class.isAssignableFrom(bean.getClass()) || BiFunction.class.isAssignableFrom(bean.getClass())) {
					Object result;
					if (BiFunction.class.isAssignableFrom(bean.getClass())) {
						result = ((BiFunction) bean).apply(adaptedInboundArguments[0], adaptedInboundArguments[1]);
					}
					else {
						result = ((Function) bean).apply(adaptedInboundArguments[0]);
					}
					result = handleCurriedFunctions(adaptedInboundArguments, result);
					if (result != null) {
						final Set<String> outputs = new TreeSet<>(kafkaStreamsBindableProxyFactory.getOutputs());
						final Iterator<String> outboundDefinitionIterator = outputs.iterator();
						if (result.getClass().isArray()) {
							final String initialInput = resolvableTypes.keySet().iterator().next();
							final StreamsBuilderFactoryBean streamsBuilderFactoryBean =
									this.kafkaStreamsBindingInformationCatalogue.getStreamsBuilderFactoryBeanPerBinding().get(initialInput);
							handleKStreamArrayOutbound(resolvableType, functionName, kafkaStreamsBindableProxyFactory,
									outboundResolvableType, (Object[]) result, streamsBuilderFactoryBean);
						}
						else {
							if (KTable.class.isAssignableFrom(result.getClass())) {
								handleSingleKStreamOutbound(resolvableTypes, outboundResolvableType != null ?
										outboundResolvableType : resolvableType.getGeneric(1), ((KTable) result).toStream(), outboundDefinitionIterator);
							}
							else {
								handleSingleKStreamOutbound(resolvableTypes, outboundResolvableType, (KStream) result, outboundDefinitionIterator);
							}
						}
					}
				}
			}
			else {
				Object result = null;
				if (resolvableType.getRawClass() != null && resolvableType.getRawClass().equals(BiFunction.class)) {
					if (composedFunctionNames != null && composedFunctionNames.length > 0) {
						result = handleComposedFunctions(adaptedInboundArguments, result, composedFunctionNames);
					}
					else {
						BiFunction<Object, Object, Object> biFunction = (BiFunction) beanFactory.getBean(functionName);
						result = biFunction.apply(adaptedInboundArguments[0], adaptedInboundArguments[1]);
						result = handleCurriedFunctions(adaptedInboundArguments, result);
					}
				}
				else {
					if (composedFunctionNames != null && composedFunctionNames.length > 0) {
						result = handleComposedFunctions(adaptedInboundArguments, result, composedFunctionNames);
					}
					else {
						Function<Object, Object> function = (Function) beanFactory.getBean(functionName);
						result = function.apply(adaptedInboundArguments[0]);
						result = handleCurriedFunctions(adaptedInboundArguments, result);
					}
				}
				if (result != null) {
					final Set<String> outputs = new TreeSet<>(kafkaStreamsBindableProxyFactory.getOutputs());
					final Iterator<String> outboundDefinitionIterator = outputs.iterator();

					if (result.getClass().isArray()) {
						final String initialInput = resolvableTypes.keySet().iterator().next();
						final StreamsBuilderFactoryBean streamsBuilderFactoryBean =
								this.kafkaStreamsBindingInformationCatalogue.getStreamsBuilderFactoryBeanPerBinding().get(initialInput);
						handleKStreamArrayOutbound(resolvableType, functionName, kafkaStreamsBindableProxyFactory,
								outboundResolvableType, (Object[]) result, streamsBuilderFactoryBean);
					}
					else {
						if (KTable.class.isAssignableFrom(result.getClass())) {
							handleSingleKStreamOutbound(resolvableTypes, outboundResolvableType != null ?
									outboundResolvableType : resolvableType.getGeneric(1), ((KTable) result).toStream(), outboundDefinitionIterator);
						}
						else {
							handleSingleKStreamOutbound(resolvableTypes, outboundResolvableType != null ?
									outboundResolvableType : resolvableType.getGeneric(1), (KStream) result, outboundDefinitionIterator);
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
	private Object handleComposedFunctions(Object[] adaptedInboundArguments, Object result, String... composedFunctionNames) {
		Object bean = beanFactory.getBean(composedFunctionNames[0]);
		if (BiFunction.class.isAssignableFrom(bean.getClass())) {
			result = ((BiFunction<Object, Object, Object>) bean).apply(adaptedInboundArguments[0], adaptedInboundArguments[1]);
		}
		else if (Function.class.isAssignableFrom(bean.getClass())) {
			result = ((Function<Object, Object>) bean).apply(adaptedInboundArguments[0]);
		}
		// If the return is a curried function, apply it
		result = handleCurriedFunctions(adaptedInboundArguments, result);
		// Apply composed functions
		return applyComposedFunctions(result, composedFunctionNames);
	}

	@SuppressWarnings({"unchecked"})
	private Object applyComposedFunctions(Object result, String[] composedFunctionNames) {
		for (int i = 1; i < composedFunctionNames.length; i++) {
			final Object bean = beanFactory.getBean(composedFunctionNames[i]);
			if (Consumer.class.isAssignableFrom(bean.getClass())) {
				((Consumer<Object>) bean).accept(result);
				result = null;
			}
			else if (Function.class.isAssignableFrom(bean.getClass())) {
				result = ((Function<Object, Object>) bean).apply(result);
			}
			else {
				throw new IllegalStateException("You can only compose functions of type either java.util.function.Function or java.util.function.Consumer.");
			}
		}
		return result;
	}

	@SuppressWarnings({"unchecked"})
	private Object handleCurriedFunctions(Object[] adaptedInboundArguments, Object result) {
		int i = 1;
		while (result instanceof Function || result instanceof Consumer) {
			if (result instanceof Function) {
				result = ((Function<Object, Object>) result).apply(adaptedInboundArguments[i]);
			}
			else {
				((Consumer<Object>) result).accept(adaptedInboundArguments[i]);
				result = null;
			}
			i++;
		}
		return result;
	}

	private void handleSingleKStreamOutbound(Map<String, ResolvableType> resolvableTypes, ResolvableType outboundResolvableType,
											KStream<Object, Object> result, Iterator<String> outboundDefinitionIterator) {
		if (outboundDefinitionIterator.hasNext()) {
			String outbound = outboundDefinitionIterator.next();
			Object targetBean = handleSingleKStreamOutbound(result, outbound);
			kafkaStreamsBindingInformationCatalogue.addOutboundKStreamResolvable(targetBean,
					outboundResolvableType);

			final String next = resolvableTypes.keySet().iterator().next();
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.kafkaStreamsBindingInformationCatalogue
					.getStreamsBuilderFactoryBeanPerBinding().get(next);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactoryPerBinding(outbound, streamsBuilderFactoryBean);
		}
	}

	private Object handleSingleKStreamOutbound(KStream<Object, Object> result, String next) {
		Object targetBean = this.applicationContext.getBean(next);
		KStreamBoundElementFactory.KStreamWrapper
				boundElement = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
		boundElement.wrap(result);
		return targetBean;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void handleKStreamArrayOutbound(ResolvableType resolvableType, String functionName,
											KafkaStreamsBindableProxyFactory kafkaStreamsBindableProxyFactory,
											ResolvableType outboundResolvableType, Object[] result,
											StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		// Binding target as the output bindings were deferred in the KafkaStreamsBindableProxyFactory
		// due to the fact that it didn't know the returned array size. At this point in the execution,
		// we know exactly the number of outbound components (from the array length), so do the binding.
		final int length = result.length;

		List<String> outputBindings = getOutputBindings(functionName, length);
		Iterator<String> iterator = outputBindings.iterator();
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

		for (Object o : result) {
			String next = iterator.next();
			kafkaStreamsBindableProxyFactory.addOutputBinding(next, KStream.class);
			RootBeanDefinition rootBeanDefinition1 = new RootBeanDefinition();
			rootBeanDefinition1.setInstanceSupplier(() -> kafkaStreamsBindableProxyFactory.getOutputHolders().get(next).getBoundTarget());
			registry.registerBeanDefinition(next, rootBeanDefinition1);

			Object targetBean = this.applicationContext.getBean(next);

			KStreamBoundElementFactory.KStreamWrapper
					boundElement = (KStreamBoundElementFactory.KStreamWrapper) targetBean;
			boundElement.wrap((KStream) o);

			kafkaStreamsBindingInformationCatalogue.addOutboundKStreamResolvable(
					targetBean, outboundResolvableType != null ? outboundResolvableType : resolvableType.getGeneric(1));

			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactoryPerBinding(next, streamsBuilderFactoryBean);
		}
	}

	private List<String> getOutputBindings(String functionName, int outputs)  {
		List<String> outputBindings = this.streamFunctionProperties.getOutputBindings(functionName);
		List<String> outputBindingNames = new ArrayList<>();
		if (!CollectionUtils.isEmpty(outputBindings)) {
			outputBindingNames.addAll(outputBindings);
			return outputBindingNames;
		}
		else {
			for (int i = 0; i < outputs; i++) {
				outputBindingNames.add(String.format("%s-%s-%d", functionName, FunctionConstants.DEFAULT_OUTPUT_SUFFIX, i));
			}
		}
		return outputBindingNames;

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
					StreamsBuilderFactoryBean streamsBuilderFactoryBean = buildStreamsBuilderAndRetrieveConfig(functionName, applicationContext,
							input, kafkaStreamsBinderConfigurationProperties, customizer, this.environment, bindingProperties);
					this.methodStreamsBuilderFactoryBeanMap.put(functionName, streamsBuilderFactoryBean);
				}
				try {
					StreamsBuilderFactoryBean streamsBuilderFactoryBean =
							this.methodStreamsBuilderFactoryBeanMap.get(functionName);
					StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
					final String applicationId = streamsBuilderFactoryBean.getStreamsConfiguration().getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
					KafkaStreamsConsumerProperties extendedConsumerProperties =
							this.kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties(input);
					extendedConsumerProperties.setApplicationId(applicationId);
					//get state store spec

					Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties, stringResolvableTypeMap.get(input));
					LOG.info("Key Serde used for " + input + ": " + keySerde.getClass().getName());
					Serde<?> valueSerde = bindingServiceProperties.getConsumerProperties(input).isUseNativeDecoding() ?
						getValueSerde(input, extendedConsumerProperties, stringResolvableTypeMap.get(input)) : Serdes.ByteArray();
					LOG.info("Value Serde used for " + input + ": " + valueSerde.getClass().getName());
					final Topology.AutoOffsetReset autoOffsetReset = getAutoOffsetReset(input, extendedConsumerProperties);

					if (parameterType.isAssignableFrom(KStream.class)) {
						KStream<?, ?> stream = getKStream(input, bindingProperties, extendedConsumerProperties,
								streamsBuilder, keySerde, valueSerde, autoOffsetReset, i == 0);
						KStreamBoundElementFactory.KStreamWrapper kStreamWrapper =
								(KStreamBoundElementFactory.KStreamWrapper) targetBean;
						//wrap the proxy created during the initial target type binding with real object (KStream)
						kStreamWrapper.wrap((KStream<Object, Object>) stream);

						this.kafkaStreamsBindingInformationCatalogue.addKeySerde((KStream<?, ?>) kStreamWrapper, keySerde);

						this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactoryPerBinding(input, streamsBuilderFactoryBean);

						if (KStream.class.isAssignableFrom(stringResolvableTypeMap.get(input).getRawClass())) {
							final Class<?> valueClass =
									(stringResolvableTypeMap.get(input).getGeneric(1).getRawClass() != null)
									? (stringResolvableTypeMap.get(input).getGeneric(1).getRawClass()) : Object.class;
							if (this.kafkaStreamsBindingInformationCatalogue.isUseNativeDecoding(
									(KStream<?, ?>) kStreamWrapper)) {
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
								streamsBuilder, extendedConsumerProperties, keySerde, valueSerde, autoOffsetReset, i == 0);
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

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}
}
