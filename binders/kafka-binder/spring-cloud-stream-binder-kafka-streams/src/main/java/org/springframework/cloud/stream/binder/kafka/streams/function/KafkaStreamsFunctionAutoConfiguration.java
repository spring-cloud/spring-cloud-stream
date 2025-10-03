/*
 * Copyright 2023-present the original author or authors.
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

import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsFunctionProcessor;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
@Configuration(proxyBeanMethods = false)
@Lazy(false)
public class KafkaStreamsFunctionAutoConfiguration {

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public KafkaStreamsFunctionProcessorInvoker kafkaStreamsFunctionProcessorInvoker(
																					KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor,
																					KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor,
																					KafkaStreamsBindableProxyFactory[] kafkaStreamsBindableProxyFactories,
																					StreamFunctionProperties streamFunctionProperties) {
		return new KafkaStreamsFunctionProcessorInvoker(kafkaStreamsFunctionBeanPostProcessor.getResolvableTypes(),
				kafkaStreamsFunctionProcessor, kafkaStreamsBindableProxyFactories, kafkaStreamsFunctionBeanPostProcessor.getMethods(),
				streamFunctionProperties);
	}

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor(StreamFunctionProperties streamFunctionProperties) {
		return new KafkaStreamsFunctionBeanPostProcessor(streamFunctionProperties);
	}
}
