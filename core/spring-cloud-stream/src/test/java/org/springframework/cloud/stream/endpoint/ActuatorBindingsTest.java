/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.cloud.stream.endpoint;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class ActuatorBindingsTest {

	/*
	 * Even though this test performs some simple assertions, the main purpose for it is to validate that
	 * it does not result in recursive exception described in https://github.com/spring-cloud/spring-cloud-stream/issues/2253
	 */
	@Test
	void test_2253() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(Bindings.class))
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
								"--spring.cloud.function.definition=consume",
								"--spring.jackson.visibility.field=ANY" // see https://github.com/spring-cloud/spring-cloud-stream/issues/2253
						// we need the above just to verify that such action does not
						// interfere with instance of ObjectMapper inside of BindingsLifecycleController
						)) {

			BindingsLifecycleController controller = context
					.getBean(BindingsLifecycleController.class);
			List<Map<?, ?>> bindings = controller.queryStates();
			assertThat(bindings.size()).isEqualTo(1);
			assertThat(bindings.get(0).get("bindingName")).isEqualTo("consume-in-0");
		}

	}

	@EnableAutoConfiguration
	public static class Bindings {

		@Bean
		public Consumer<String> consume() {
			return message -> System.out.println("Received message " + message);
		}

	}

}
