/*
 * Copyright 2019-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBinderUtils;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.ResolvableType;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;

/**
 * @author Soby Chacko
 * @author James Forward
 *
 * @since 2.2.0
 */
public class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean, BeanFactoryAware {

	private static final Log LOG = LogFactory.getLog(KafkaStreamsFunctionBeanPostProcessor.class);

	private static final String[] EXCLUDE_FUNCTIONS = new String[]{"functionRouter", "sendToDlqAndContinue"};

	private ConfigurableListableBeanFactory beanFactory;
	private boolean onlySingleFunction;
	private final Map<String, ResolvableType> resolvableTypeMap = new TreeMap<>();
	private final Map<String, Method> methods = new TreeMap<>();

	private final StreamFunctionProperties streamFunctionProperties;

	private final Map<String, ResolvableType> kafkaStreamsOnlyResolvableTypes = new HashMap<>();
	private final Map<String, Method> kafakStreamsOnlyMethods = new HashMap<>();

	public KafkaStreamsFunctionBeanPostProcessor(StreamFunctionProperties streamFunctionProperties) {
		this.streamFunctionProperties = streamFunctionProperties;
	}

	public Map<String, ResolvableType> getResolvableTypes() {
		return this.resolvableTypeMap;
	}

	public Map<String, Method> getMethods() {
		return methods;
	}

	@Override
	public void afterPropertiesSet() {
		String[] functionNames = this.beanFactory.getBeanNamesForType(Function.class);
		String[] biFunctionNames = this.beanFactory.getBeanNamesForType(BiFunction.class);
		String[] consumerNames = this.beanFactory.getBeanNamesForType(Consumer.class);
		String[] biConsumerNames = this.beanFactory.getBeanNamesForType(BiConsumer.class);

		final Stream<String> concat = Stream.concat(
				Stream.concat(Stream.of(functionNames), Stream.of(consumerNames)),
				Stream.concat(Stream.of(biFunctionNames), Stream.of(biConsumerNames)));
		final List<String> collect = concat.collect(Collectors.toList());
		collect.removeIf(s -> Arrays.stream(EXCLUDE_FUNCTIONS).anyMatch(t -> t.equals(s)));
		collect.removeIf(Pattern.compile(".*_registration").asPredicate());

		onlySingleFunction = collect.size() == 1;
		collect.stream()
				.forEach(this::extractResolvableTypes);

		kafkaStreamsOnlyResolvableTypes.keySet().forEach(k -> addResolvableTypeInfo(k, kafkaStreamsOnlyResolvableTypes.get(k)));
		kafakStreamsOnlyMethods.keySet().forEach(k -> addResolvableTypeInfo(k, kafakStreamsOnlyMethods.get(k)));

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

		final String definition = streamFunctionProperties.getDefinition();
		final String[] functionUnits = KafkaStreamsBinderUtils.deriveFunctionUnits(definition);

		final Set<String> kafkaStreamsMethodNames = new HashSet<>(kafkaStreamsOnlyResolvableTypes.keySet());
		kafkaStreamsMethodNames.addAll(this.resolvableTypeMap.keySet());

		if (functionUnits.length == 0) {
			for (String s : getResolvableTypes().keySet()) {
				ResolvableType[] resolvableTypes = new ResolvableType[]{getResolvableTypes().get(s)};
				RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
						KafkaStreamsBindableProxyFactory.class);
				registerKakaStreamsProxyFactory(registry, s, resolvableTypes, rootBeanDefinition);
			}
		}
		else {
			for (String functionUnit : functionUnits) {
				if (functionUnit.contains("|")) {
					final String[] composedFunctions = functionUnit.split("\\|");
					String derivedNameFromComposed = "";
					ResolvableType[] resolvableTypes = new ResolvableType[composedFunctions.length];

					int i = 0;
					boolean nonKafkaStreamsFunctionsFound = false;

					for (String split : composedFunctions) {
						derivedNameFromComposed = derivedNameFromComposed.concat(split);
						resolvableTypes[i++] = getResolvableTypes().get(split);
						if (!kafkaStreamsMethodNames.contains(split)) {
							nonKafkaStreamsFunctionsFound = true;
							break;
						}
					}
					if (!nonKafkaStreamsFunctionsFound) {
						RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
								KafkaStreamsBindableProxyFactory.class);
						registerKakaStreamsProxyFactory(registry, derivedNameFromComposed, resolvableTypes, rootBeanDefinition);
					}
				}
				else {
					// Ensure that the function unit is a Kafka Streams function
					if (kafkaStreamsMethodNames.contains(functionUnit)) {
						ResolvableType[] resolvableTypes = new ResolvableType[]{getResolvableTypes().get(functionUnit)};
						RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
								KafkaStreamsBindableProxyFactory.class);
						registerKakaStreamsProxyFactory(registry, functionUnit, resolvableTypes, rootBeanDefinition);
					}
				}
			}
		}
	}

	private void registerKakaStreamsProxyFactory(BeanDefinitionRegistry registry, String s, ResolvableType[] resolvableTypes, RootBeanDefinition rootBeanDefinition) {
		rootBeanDefinition.getConstructorArgumentValues()
				.addGenericArgumentValue(resolvableTypes);
		rootBeanDefinition.getConstructorArgumentValues()
				.addGenericArgumentValue(s);
		rootBeanDefinition.getConstructorArgumentValues()
				.addGenericArgumentValue(getMethods().get(s));
		registry.registerBeanDefinition("kafkaStreamsBindableProxyFactory-" + s, rootBeanDefinition);
	}

	private void extractResolvableTypes(String key) {
		final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
						this.beanFactory.getBeanDefinition(key))
						.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());
		try {
			Method[] methods = classObj.getDeclaredMethods();
			Optional<Method> functionalBeanMethods = KafkaStreamsBinderUtils.findMethodWithName(key, methods);
			if (functionalBeanMethods.isEmpty()) {
				methods = classObj.getMethods(); // check the inherited methods
				functionalBeanMethods = KafkaStreamsBinderUtils.findMethodWithName(key, methods);
			}
			if (functionalBeanMethods.isEmpty()) {
				final BeanDefinition beanDefinition = this.beanFactory.getBeanDefinition(key);
				final String factoryMethodName = beanDefinition.getFactoryMethodName();
				functionalBeanMethods = KafkaStreamsBinderUtils.findMethodWithName(factoryMethodName, methods);
			}

			if (functionalBeanMethods.isPresent()) {
				Method method = functionalBeanMethods.get();
				ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
				final Class<?> rawClass = resolvableType.getGeneric(0).getRawClass();
				if (rawClass == KStream.class || rawClass == KTable.class || rawClass == GlobalKTable.class) {
					if (onlySingleFunction) {
						resolvableTypeMap.put(key, resolvableType);
					}
					else {
						discoverOnlyKafkaStreamsResolvableTypes(key, resolvableType);
					}
				}
			}
			else {
				Optional<Method> componentBeanMethods = Arrays.stream(methods)
						.filter(m -> m.getName().equals("apply") && isKafkaStreamsTypeFound(m) ||
								m.getName().equals("accept") && isKafkaStreamsTypeFound(m)).findFirst();
				if (componentBeanMethods.isPresent()) {
					Method method = componentBeanMethods.get();
					final ResolvableType resolvableType = ResolvableType.forMethodParameter(method, 0);
					final Class<?> rawClass = resolvableType.getRawClass();
					if (rawClass == KStream.class || rawClass == KTable.class || rawClass == GlobalKTable.class) {
						if (onlySingleFunction) {
							resolvableTypeMap.put(key, resolvableType);
							this.methods.put(key, method);
						}
						else {
							discoverOnlyKafkaStreamsResolvableTypesAndMethods(key, resolvableType, method);
						}
					}
				}
			}
		}
		catch (Exception e) {
			LOG.error("Function activation issues while mapping the function: " + key, e);
		}
	}

	private void addResolvableTypeInfo(String key, ResolvableType resolvableType) {
		if (kafkaStreamsOnlyResolvableTypes.size() == 1) {
			resolvableTypeMap.put(key, resolvableType);
		}
		else {
			final String definition = streamFunctionProperties.getDefinition();
			if (definition == null) {
				throw new IllegalStateException("Multiple functions found, but function definition property is not set.");
			}
			else if (definition.contains(key)) {
				resolvableTypeMap.put(key, resolvableType);
			}
		}
	}

	private void discoverOnlyKafkaStreamsResolvableTypes(String key, ResolvableType resolvableType) {
		kafkaStreamsOnlyResolvableTypes.put(key, resolvableType);
	}

	private void discoverOnlyKafkaStreamsResolvableTypesAndMethods(String key, ResolvableType resolvableType, Method method) {
		kafkaStreamsOnlyResolvableTypes.put(key, resolvableType);
		kafakStreamsOnlyMethods.put(key, method);
	}

	private void addResolvableTypeInfo(String key, Method method) {
		if (kafakStreamsOnlyMethods.size() == 1) {
			this.methods.put(key, method);
		}
		else {
			final String definition = streamFunctionProperties.getDefinition();
			if (definition == null) {
				throw new IllegalStateException("Multiple functions found, but function definition property is not set.");
			}
			else if (definition.contains(key)) {
				this.methods.put(key, method);
			}
		}
	}

	private boolean isKafkaStreamsTypeFound(Method method) {
		return KStream.class.isAssignableFrom(method.getParameters()[0].getType()) ||
				KTable.class.isAssignableFrom(method.getParameters()[0].getType()) ||
				GlobalKTable.class.isAssignableFrom(method.getParameters()[0].getType());
	}

	@Override
	public void setBeanFactory(@NonNull BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}
}
