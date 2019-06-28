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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

/**
 * Custom {@link org.springframework.context.annotation.Condition} that detects the presence
 * of java.util.Function|Consumer beans. Used for Kafka Streams function support.
 *
 * @author Soby Chakco
 * @since 2.2.0
 */
public class FunctionDetectorCondition extends SpringBootCondition {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
		if (context != null &&  context.getBeanFactory() != null) {
			Map functionTypes = context.getBeanFactory().getBeansOfType(Function.class);
			functionTypes.putAll(context.getBeanFactory().getBeansOfType(Consumer.class));
			functionTypes.putAll(context.getBeanFactory().getBeansOfType(BiFunction.class));
			final Map<String, Object> kstreamFunctions = pruneFunctionBeansForKafkaStreams(functionTypes, context);

			if (!kstreamFunctions.isEmpty()) {
				return ConditionOutcome.match("Matched. Function/BiFunction/Consumer beans found");
			}
			else {
				return ConditionOutcome.noMatch("No match. No Function/BiFunction/Consumer beans found");
			}
		}
		return ConditionOutcome.noMatch("No match. No Function/BiFunction/Consumer beans found");
	}

	private static <T> Map<String, T> pruneFunctionBeansForKafkaStreams(Map<String, T> originalFunctionBeans,
																		ConditionContext context) {
		final Map<String, T> prunedMap = new HashMap<>();

		for (String key : originalFunctionBeans.keySet()) {
			final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
							context.getBeanFactory().getBeanDefinition(key))
							.getMetadata().getClassName(),
					ClassUtils.getDefaultClassLoader());
			try {
				Method method = classObj.getMethod(key);
				ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
				final Class<?> rawClass = resolvableType.getGeneric(0).getRawClass();
				if (rawClass == KStream.class || rawClass == KTable.class || rawClass == GlobalKTable.class) {
					prunedMap.put(key, originalFunctionBeans.get(key));
				}
			}
			catch (NoSuchMethodException e) {
				//ignore
			}
		}
		return prunedMap;
	}
}
