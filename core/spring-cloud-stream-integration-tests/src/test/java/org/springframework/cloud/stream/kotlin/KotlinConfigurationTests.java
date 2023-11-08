/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.cloud.stream.kotlin;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

class KotlinConfigurationTests {

	@Test
	void kotlinSupplierPollableBean() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(KotlinTestConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=produceNames")) {

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(1000, "produceNames-out-0");
			assertThat(result.getPayload()).isEqualTo("Ricky".getBytes());
			result = output.receive(1000, "produceNames-out-0");
			assertThat(result.getPayload()).isEqualTo("Julien".getBytes());
			result = output.receive(1000, "produceNames-out-0");
			assertThat(result.getPayload()).isEqualTo("Bubbles".getBytes());
		}
	}
}
