/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
public class ErrorBindingTests {

	@Test
	void testConfigurationWithDefaultErrorHandler() {
		ApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ErrorBindingTests.ErrorConfigurationDefault.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.bindings.handle-in-0.consumer.max-attempts=1",
										"--spring.cloud.function.definition=handle",
										"--spring.cloud.stream.default.error-handler-definition=errorHandler",
										"--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));

		ErrorConfigurationDefault errorConfiguration = context
				.getBean(ErrorConfigurationDefault.class);
		assertThat(errorConfiguration.counter).isEqualTo(6);
	}

	@Test
	void testConfigurationWithBindingSpecificErrorHandler() {
		ApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ErrorBindingTests.ErrorConfigurationWithCustomErrorHandler.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.bindings.handle-in-0.consumer.max-attempts=1",
										"--spring.cloud.function.definition=handle",
										"--spring.cloud.stream.bindings.handle-in-0.error-handler-definition=errorHandler",
										"--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));

		ErrorConfigurationWithCustomErrorHandler errorConfiguration = context
				.getBean(ErrorConfigurationWithCustomErrorHandler.class);
		assertThat(errorConfiguration.counter).isEqualTo(6);
	}

	@EnableAutoConfiguration
	public static class TestProcessor {

		@Bean
		public Function<String, String> processor() {
			return s -> s;
		}
	}

	@EnableAutoConfiguration
	public static class ErrorConfigurationDefault {

		private int counter;

		@Bean
		public Consumer<Object> handle() {
			return v -> {
				this.counter++;
				throw new RuntimeException("BOOM!");
			};
		}

		@Bean
		public Consumer<Object> errorHandler() {
			return v -> {
				this.counter++;
			};
		}

	}

	@EnableAutoConfiguration
	public static class ErrorConfigurationWithCustomErrorHandler {

		private int counter;

		@Bean
		public Consumer<Object> handle() {
			return v -> {
				this.counter++;
				throw new RuntimeException("BOOM!");
			};
		}

		@Bean
		public Consumer<Object> errorHandler() {
			return v -> {
				this.counter++;
			};
		}
	}

}
