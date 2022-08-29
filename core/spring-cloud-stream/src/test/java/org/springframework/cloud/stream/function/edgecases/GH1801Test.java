/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.function.edgecases;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

/**
 * This test validates that the issue https://github.com/spring-cloud/spring-cloud-stream/issues/1801
 * is addressed.
 *
 * @author Oleg Zhurakousky
 *
 */
public class GH1801Test {

	@Test
	public void test() {
		SampleBootApplication.main("--spring.cloud.stream.defaultBinder=integration");
	}

	@SpringBootApplication
	public static class SampleBootApplication {
		public static void main(String... args) {
			new SpringApplicationBuilder(SampleBootApplication.class).web(WebApplicationType.NONE).run(args);
		}
	}
}

@Configuration
class StreamConfiguration {

	@Bean
	public Consumer<Message<?>> consumer() {
		return System.out::println;
	}
}
