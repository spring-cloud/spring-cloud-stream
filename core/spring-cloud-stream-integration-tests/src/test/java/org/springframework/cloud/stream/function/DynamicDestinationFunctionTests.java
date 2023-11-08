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

package org.springframework.cloud.stream.function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 * @author David Turanski
 *
 *
 * TODO: Need to rewrite this test.
 */
class DynamicDestinationFunctionTests {

	@AfterAll
	public static void after() {
		System.clearProperty("spring.cloud.stream.function.definition");
		System.clearProperty("spring.cloud.function.definition");
	}

	@Test
	@Disabled
	void emptyConfiguration() {
		TestChannelBinderConfiguration.applicationContextRunner(SampleConfiguration.class)
			.withPropertyValues(
				"spring.jmx.enabled=false",
				"spring.cloud.stream.bindings.fooDestination.producer.partitionKeyExtractorName=keyExtractor")
			.run(context -> {
				InputDestination input = context.getBean(InputDestination.class);
				input.send(new GenericMessage<String>("fooDestination"));

				BindingServiceProperties serviceProperties = context.getBean(BindingServiceProperties.class);
				assertThat("keyExtractor").isEqualTo(
					serviceProperties.getProducerProperties("fooDestination").getPartitionKeyExtractorName());

				OutputDestination output = context.getBean(OutputDestination.class);
				Object result = output.receive(1000).getPayload();
				assertThat(result).isEqualTo("fooDestination");
			});
	}

	@EnableAutoConfiguration
	public static class SampleConfiguration {

		@Bean
		public PartitionKeyExtractorStrategy keyExtractor() {
			return message -> 0;
		}

	}

}
