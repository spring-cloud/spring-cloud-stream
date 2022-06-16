/*
 * Copyright 2022-2022 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * {@link EnvironmentPostProcessor} to exclude the SendToDlqAndContinue BiFunction
 * in core Spring Cloud Function by adding it to the ineligible function definitions.
 *
 * @author Soby Chacko
 */
public class KafkaStreamsBinderEnvironmentPostProcessor implements EnvironmentPostProcessor {

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment,
									SpringApplication application) {
		Map<String, Object> kafkaStreamsBinderIneligibleDefns = new HashMap<>();
		kafkaStreamsBinderIneligibleDefns.put("spring.cloud.function.ineligible-definitions",
			"sendToDlqAndContinue");

		environment.getPropertySources().addLast(new MapPropertySource(
			"KAFKA_STREAMS_BINDER_INELIGIBLE_DEFINITIONS", kafkaStreamsBinderIneligibleDefns));
	}

}
