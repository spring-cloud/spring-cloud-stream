/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
class FluxMessageChannelBindingTests {

	@Test
	void fluxMessageChannelBindingWhenReactiveOptIn() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.reactive.uppercase=true")) {
			assertThat(context.getBean("uppercase-in-0", MessageChannel.class)).isInstanceOf(FluxMessageChannel.class);
			assertThat(context.getBean("uppercase-out-0", MessageChannel.class)).isInstanceOf(FluxMessageChannel.class);
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class ReactiveFunctionConfiguration {

		@Bean
		public Function<Flux<String>, Flux<String>> uppercase() {
			return s -> s.map(String::toUpperCase);
		}
	}
}
