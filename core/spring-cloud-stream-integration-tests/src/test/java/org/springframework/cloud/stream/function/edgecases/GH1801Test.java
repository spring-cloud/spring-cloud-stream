/*
 * Copyright 2019-present the original author or authors.
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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.messaging.Message;
import org.springframework.util.ObjectUtils;

/**
 * This test validates that the issue https://github.com/spring-cloud/spring-cloud-stream/issues/1801
 * is addressed.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 */
class GH1801Test {

	private static ClassLoader createClassLoader(String[] additionalClasspathDirectories) throws IOException {
		URL[] urls = ObjectUtils.isEmpty(additionalClasspathDirectories) ? new URL[0]
				: new URL[additionalClasspathDirectories.length];
		if (!ObjectUtils.isEmpty(additionalClasspathDirectories)) {
			for (int i = 0; i < additionalClasspathDirectories.length; i++) {
				urls[i] = new URL(new ClassPathResource(additionalClasspathDirectories[i]).getURL().toString() + "/");
			}
		}
		return new URLClassLoader(urls, GH1801Test.class.getClassLoader());
	}

	@Test
	void verifyNoNPEWhenFactoryMethodNull() throws Exception {
		SampleBootApplication.main("--spring.cloud.stream.defaultBinder=integration1");
	}

	@EnableAutoConfiguration
	public static class SampleBootApplication {
		public static void main(String... args) throws Exception {
			ClassLoader classLoader = createClassLoader(new String[] { "binder1" });
			new SpringApplicationBuilder(SampleBootApplication.class)
			.resourceLoader(new DefaultResourceLoader(classLoader))
			.web(WebApplicationType.NONE).run(args);
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
