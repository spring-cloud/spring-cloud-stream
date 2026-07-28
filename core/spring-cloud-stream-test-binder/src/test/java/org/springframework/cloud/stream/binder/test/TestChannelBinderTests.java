/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.lang.reflect.Method;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 * @author Mattia Gualtieri
 *
 */
class TestChannelBinderTests {

	@Test
	public void test() throws Exception {
		// nothing to assert. no failure on this test signifies success
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SampleConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.function-in-0.destination=input")) {
			TestChannelBinder binder = context.getBean(TestChannelBinder.class);
			Method registerErrorInfrastructure = ReflectionUtils
				.findMethod(TestChannelBinder.class, "registerErrorInfrastructure", ProducerDestination.class, String.class, boolean.class);
			registerErrorInfrastructure.setAccessible(true);
			ProducerDestination destination = new ProducerDestination() {
				@Override
				public String getNameForPartition(int partition) {
					return "sample";
				}

				@Override
				public String getName() {
					return "sample";
				}
			};
			registerErrorInfrastructure.invoke(binder, destination, "function-in-0", false);
			destination = new ProducerDestination() {
				@Override
				public String getNameForPartition(int partition) {
					return "sample";
				}

				@Override
				public String getName() {
					return "sample";
				}
			};
			registerErrorInfrastructure.invoke(binder, destination, "function-in-0", false);
		}
	}

	@Test
	void stoppedBindingDoesNotConsumeAndRestartedBindingConsumesAgain() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EchoConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=echo",
				"--spring.cloud.stream.bindings.echo-in-0.group=test-group")) {

			InputDestination input = context.getBean(InputDestination.class);
			OutputDestination output = context.getBean(OutputDestination.class);
			BindingsLifecycleController controller = context.getBean(BindingsLifecycleController.class);

			input.send(new GenericMessage<>("first".getBytes()), "echo-in-0");
			assertThat(output.receive(1000, "echo-out-0").getPayload()).isEqualTo("first".getBytes());

			controller.changeState("echo-in-0", State.STOPPED);
			assertThat(controller.queryState("echo-in-0").get(0).isRunning()).isFalse();

			input.send(new GenericMessage<>("while-stopped".getBytes()), "echo-in-0");
			// A stale subscription left behind by an incomplete stop() would still deliver
			// this message and produce an echo, even though the binding is reported as stopped.
			assertThat(output.receive(200, "echo-out-0")).isNull();

			controller.changeState("echo-in-0", State.STARTED);
			assertThat(controller.queryState("echo-in-0").get(0).isRunning()).isTrue();

			input.send(new GenericMessage<>("after-restart".getBytes()), "echo-in-0");
			assertThat(output.receive(1000, "echo-out-0").getPayload()).isEqualTo("after-restart".getBytes());
		}
	}

	@EnableAutoConfiguration
	public static class SampleConfiguration {

	}

	@EnableAutoConfiguration
	public static class EchoConfiguration {

		@Bean
		public Function<String, String> echo() {
			return value -> value;
		}
	}
}
