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

import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsFunctionProcessor;
import org.springframework.core.ResolvableType;

/**
 *
 * @author Soby Chacko
 * @since 2.1.0
 */
public class KafkaStreamsFunctionProcessorInvoker {

	private final KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor;
	private final Map<String, ResolvableType> resolvableTypeMap;

	public KafkaStreamsFunctionProcessorInvoker(Map<String, ResolvableType> resolvableTypeMap,
										KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor) {
		this.kafkaStreamsFunctionProcessor = kafkaStreamsFunctionProcessor;
		this.resolvableTypeMap = resolvableTypeMap;
	}

	@PostConstruct
	void invoke() {
		resolvableTypeMap.forEach((key, value) ->
				this.kafkaStreamsFunctionProcessor.setupFunctionInvokerForKafkaStreams(value, key));
	}
}
