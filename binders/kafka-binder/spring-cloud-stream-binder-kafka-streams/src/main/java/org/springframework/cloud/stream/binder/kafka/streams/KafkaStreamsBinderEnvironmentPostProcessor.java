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

import java.util.Collections;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * {@link EnvironmentPostProcessor} to ensure the {@link SendToDlqAndContinue sendToDlqAndContinue} BiFunction
 * is excluded in core Spring Cloud Function by adding it to the ineligible function definitions.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class KafkaStreamsBinderEnvironmentPostProcessor implements EnvironmentPostProcessor {

	/**
	 * The bean name of the SendToDlqAndContinue function - must remain in sync w/
	 * {@link KafkaStreamsBinderSupportAutoConfiguration#sendToDlqAndContinue()}.
	 */
	private static final String SEND_TO_DLQ_AND_CONTINUE_BEAN_NAME = "sendToDlqAndContinue";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		String ineligibleDefinitionsPropertyKey = "spring.cloud.function.ineligible-definitions";
		String ineligibleDefinitions = SEND_TO_DLQ_AND_CONTINUE_BEAN_NAME;
		if (environment.getProperty(ineligibleDefinitionsPropertyKey) != null) {
			ineligibleDefinitions += ("," + environment.getProperty(ineligibleDefinitionsPropertyKey));
		}
		environment.getPropertySources().addFirst(new MapPropertySource(
				"kafkaStreamsBinderIneligibleDefinitions",
				Collections.singletonMap(ineligibleDefinitionsPropertyKey, ineligibleDefinitions)));
	}

}
