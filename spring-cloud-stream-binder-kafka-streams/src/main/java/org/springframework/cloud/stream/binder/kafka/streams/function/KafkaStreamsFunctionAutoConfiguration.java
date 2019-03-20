/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsFunctionProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Soby Chacko
 */
@Configuration
@ConditionalOnProperty("spring.cloud.stream.kafka.streams.function.definition")
@EnableConfigurationProperties(KafkaStreamsFunctionProperties.class)
public class KafkaStreamsFunctionAutoConfiguration {

	@Autowired
	ConfigurableApplicationContext context;

	@Bean
	public KafkaStreamsFunctionProcessorInvoker kafkaStreamsFunctionProcessorInvoker(
																					KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor,
																					KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor,
																					KafkaStreamsFunctionProperties properties) {
		return new KafkaStreamsFunctionProcessorInvoker(kafkaStreamsFunctionBeanPostProcessor.getResolvableType(),
				properties.getDefinition(), kafkaStreamsFunctionProcessor);
	}

	@Bean
	public KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor(
			ConfigurableApplicationContext context, KafkaStreamsFunctionProperties properties) {
		return new KafkaStreamsFunctionBeanPostProcessor(properties, context);
	}

}
