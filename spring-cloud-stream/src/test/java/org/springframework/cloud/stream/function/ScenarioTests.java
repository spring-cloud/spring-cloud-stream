/*
 * Copyright 2021-2021 the original author or authors.
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

import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class ScenarioTests {

	@Test
	public void test2107() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(FunctionReturningNullConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=uppercase")) {

			InputDestination input = context.getBean(InputDestination.class);
			input.send(new GenericMessage<byte[]>("a".getBytes()), "uppercase-in-0");
			OutputDestination output = context.getBean(OutputDestination.class);
			assertThat(new String(output.receive(2000, "uppercase-out-0").getPayload())).isEqualTo("a");
			input.send(new GenericMessage<byte[]>("b".getBytes()), "uppercase-in-0");
			assertThat(output.receive(2000, "uppercase-out-0")).isNull();
		}
	}

	@Test
	public void test2112() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(TestConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false",
								"--spring.cloud.function.definition=messageFunction")) {

			InputDestination input = context.getBean(InputDestination.class);
			OutputDestination output = context.getBean(OutputDestination.class);

			input.send(new GenericMessage<byte[]>("hello-1".getBytes()), "messageFunction-in-0");
			output.clear("messageFunction-out-0");
			input.send(new GenericMessage<byte[]>("hello-2".getBytes()), "messageFunction-in-0");
			assertThat(new String(output.receive(1000, "messageFunction-out-0").getPayload())).isEqualTo("hello-2");

			input.send(new GenericMessage<byte[]>("hello-1".getBytes()), "messageFunction-in-0");
			input.send(new GenericMessage<byte[]>("hello-2".getBytes()), "messageFunction-in-0");
			output.clear();
			input.send(new GenericMessage<byte[]>("hello-3".getBytes()), "messageFunction-in-0");
			assertThat(new String(output.receive(1000, "messageFunction-out-0").getPayload())).isEqualTo("hello-3");
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class TestConfiguration {
		@Bean
		public Function<Message<String>, Message<String>> messageFunction() {
			return v -> v;
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class FunctionReturningNullConfiguration {
		@Bean
		public Function<String, String> uppercase() {
			return v -> {
				if ("a".equals(v)) {
					return v;
				}
				else {
					return null;
				}
			};
		}
	}
}
