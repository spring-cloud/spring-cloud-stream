/*
 * Copyright 2019-present the original author or authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBinderUtils;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * Custom {@link org.springframework.context.annotation.Condition} that detects the presence
 * of java.util.Function|Consumer beans. Used for Kafka Streams function support.
 *
 * @author Soby Chacko
 * @since 2.2.0
 */
public class FunctionDetectorCondition extends SpringBootCondition {

	private static final Log LOG = LogFactory.getLog(FunctionDetectorCondition.class);

	@Override
	public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
		if (context != null &&  context.getBeanFactory() != null) {

			String[] functionTypes = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context.getBeanFactory(), Function.class, true, false);
			String[] consumerTypes = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context.getBeanFactory(), Consumer.class, true, false);
			String[] biFunctionTypes = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context.getBeanFactory(), BiFunction.class, true, false);
			String[] biConsumerTypes = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context.getBeanFactory(), BiConsumer.class, true, false);

			List<String> functionComponents = new ArrayList<>();

			functionComponents.addAll(Arrays.asList(functionTypes));
			functionComponents.addAll(Arrays.asList(consumerTypes));
			functionComponents.addAll(Arrays.asList(biFunctionTypes));
			functionComponents.addAll(Arrays.asList(biConsumerTypes));

			List<String> kafkaStreamsFunctions = pruneFunctionBeansForKafkaStreams(functionComponents, context);
			if (!CollectionUtils.isEmpty(kafkaStreamsFunctions)) {
				return ConditionOutcome.match("Matched. Function/BiFunction/Consumer beans found");
			}
			else {
				return ConditionOutcome.noMatch("No match. No Function/BiFunction/Consumer beans found");
			}
		}
		return ConditionOutcome.noMatch("No match. No Function/BiFunction/Consumer beans found");
	}

	private static List<String> pruneFunctionBeansForKafkaStreams(List<String> functionComponents,
																		ConditionContext context) {
		final List<String> prunedList = new ArrayList<>();

		for (String key : functionComponents) {
			final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
							Objects.requireNonNull(context.getBeanFactory()).getBeanDefinition(key))
							.getMetadata().getClassName(),
					ClassUtils.getDefaultClassLoader());
			try {

				Method[] methods = classObj.getDeclaredMethods();
				Optional<Method> kafkaStreamMethod = KafkaStreamsBinderUtils.findMethodWithName(key, methods);
				if (kafkaStreamMethod.isEmpty()) {
					// check any inherited methods
					methods = classObj.getMethods();
					kafkaStreamMethod = KafkaStreamsBinderUtils.findMethodWithName(key, methods);
				}
				if (kafkaStreamMethod.isEmpty()) {
					// check if the bean name is overridden.
					final BeanDefinition beanDefinition = context.getBeanFactory().getBeanDefinition(key);
					final String factoryMethodName = beanDefinition.getFactoryMethodName();
					kafkaStreamMethod = KafkaStreamsBinderUtils.findMethodWithName(factoryMethodName, methods);
				}

				if (kafkaStreamMethod.isPresent()) {
					Method method = kafkaStreamMethod.get();
					ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
					final Class<?> rawClass = resolvableType.getGeneric(0).getRawClass();
					if (rawClass == KStream.class || rawClass == KTable.class || rawClass == GlobalKTable.class) {
						prunedList.add(key);
					}
				}
				else {
					//check if it is a @Component bean.
					Optional<Method> componentBeanMethod = Arrays.stream(methods).filter(
							m -> (m.getName().equals("apply") || m.getName().equals("accept"))
									&& isKafkaStreamsTypeFound(m)).findFirst();
					if (componentBeanMethod.isPresent()) {
						Method method = componentBeanMethod.get();
						final ResolvableType resolvableType1 = ResolvableType.forMethodParameter(method, 0);
						final Class<?> rawClass = resolvableType1.getRawClass();
						if (rawClass == KStream.class || rawClass == KTable.class || rawClass == GlobalKTable.class) {
							prunedList.add(key);
						}
					}
				}
			}
			catch (Exception e) {
				LOG.error("Function not found: " + key, e);
			}
		}
		return prunedList;
	}

	private static boolean isKafkaStreamsTypeFound(Method method) {
		return KStream.class.isAssignableFrom(method.getParameters()[0].getType()) ||
				KTable.class.isAssignableFrom(method.getParameters()[0].getType()) ||
				GlobalKTable.class.isAssignableFrom(method.getParameters()[0].getType());
	}
}
