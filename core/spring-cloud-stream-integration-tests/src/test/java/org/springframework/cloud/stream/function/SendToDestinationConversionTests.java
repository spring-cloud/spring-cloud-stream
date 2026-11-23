/*
 * Copyright 2026-present the original author or authors.
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
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GH-2719: a POJO payload sent with the sendto destination header must be converted
 * the same way as the output of a regular function.
 */
class SendToDestinationConversionTests {

	@Test
	void plainFunctionDynamicDestination() {
		assertConverted(Config.class, "plain-in", "plain-in-0", null,
			"--spring.cloud.function.definition=plain");
	}

	@Test
	void plainFunctionDeclaredOutputBinding() {
		assertConverted(Config.class, "plain-in", "plain-in-0", null,
			"--spring.cloud.function.definition=plain", "--spring.cloud.stream.output-bindings=out");
	}

	@Test
	void routingFunctionDynamicDestination() {
		assertConverted(Config.class, "route", "functionRouter-in-0", "RoutedEvent",
			"--spring.cloud.function.definition=functionRouter",
			"--spring.cloud.stream.function.routing.enabled=true",
			"--spring.cloud.function.routing-expression=headers['Type']");
	}

	@Test
	void routingFunctionDeclaredOutputBinding() {
		assertConverted(Config.class, "route", "functionRouter-in-0", "RoutedEvent",
			"--spring.cloud.function.definition=functionRouter",
			"--spring.cloud.stream.function.routing.enabled=true",
			"--spring.cloud.function.routing-expression=headers['Type']",
			"--spring.cloud.stream.output-bindings=out");
	}

	private void assertConverted(Class<?> config, String payload, String inputBinding, String type, String... args) {
		String[] all = new String[args.length + 1];
		all[0] = "--spring.jmx.enabled=false";
		System.arraycopy(args, 0, all, 1, args.length);
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(config))
			.web(WebApplicationType.NONE).run(all)) {

			MessageBuilder<byte[]> builder = MessageBuilder.withPayload(payload.getBytes());
			if (type != null) {
				builder.setHeader("Type", type);
			}
			context.getBean(InputDestination.class).send(builder.build(), inputBinding);

			Message<?> received = context.getBean(OutputDestination.class).receive(2000, "out");
			assertThat(received).as("message sent to 'out'").isNotNull();
			assertThat(received.getPayload()).as("payload type " + received.getPayload().getClass().getName())
				.isInstanceOf(byte[].class);
			assertThat(new String((byte[]) received.getPayload())).contains("\"name\"");
		}
	}

	public static class Foo {

		private String name;

		public Foo() {
		}

		public Foo(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	@EnableAutoConfiguration
	public static class Config {

		@Bean
		public Function<String, Message<Foo>> plain() {
			return in -> MessageBuilder.withPayload(new Foo(in))
				.setHeader("spring.cloud.stream.sendto.destination", "out").build();
		}

		@Bean("RoutedEvent")
		public Function<String, Message<Foo>> routed() {
			return in -> MessageBuilder.withPayload(new Foo(in))
				.setHeader("spring.cloud.stream.sendto.destination", "out").build();
		}

	}

}
