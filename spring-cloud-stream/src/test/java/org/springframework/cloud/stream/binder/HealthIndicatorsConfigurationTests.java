/*
 * Copyright 2016-2017 the original author or authors.
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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicatorRegistry;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.stub1.StubBinder1;
import org.springframework.cloud.stream.binder.stub2.StubBinder2;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ObjectUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class HealthIndicatorsConfigurationTests {

	public static ConfigurableApplicationContext createBinderTestContext(
			String[] additionalClasspathDirectories, String... properties)
			throws IOException {
		URL[] urls = ObjectUtils.isEmpty(additionalClasspathDirectories) ? new URL[0]
				: new URL[additionalClasspathDirectories.length];
		if (!ObjectUtils.isEmpty(additionalClasspathDirectories)) {
			for (int i = 0; i < additionalClasspathDirectories.length; i++) {
				urls[i] = new URL(new ClassPathResource(additionalClasspathDirectories[i])
						.getURL().toString() + "/");
			}
		}
		ClassLoader classLoader = new URLClassLoader(urls,
				BinderFactoryConfigurationTests.class.getClassLoader());

		return new SpringApplicationBuilder(SimpleSource.class)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties).web(WebApplicationType.NONE).run();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void healthIndicatorsCheck() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1", "binder2" },
				"spring.cloud.stream.defaultBinder:binder2",
				"--spring.jmx.enabled=false");
		Binder binder1 = context.getBean(BinderFactory.class).getBinder("binder1",
				MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		Binder binder2 = context.getBean(BinderFactory.class).getBinder("binder2",
				MessageChannel.class);
		assertThat(binder2).isInstanceOf(StubBinder2.class);
		CompositeHealthIndicator bindersHealthIndicator = context
				.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(
				bindersHealthIndicator);
		assertThat(bindersHealthIndicator).isNotNull();
		assertThat(
				context.getBean("test1HealthIndicator1", CompositeHealthIndicator.class))
						.isNotNull();
		assertThat(
				context.getBean("test2HealthIndicator2", CompositeHealthIndicator.class))
						.isNotNull();

		HealthIndicatorRegistry registry = (HealthIndicatorRegistry) directFieldAccessor
				.getPropertyValue("registry");

		Map<String, HealthIndicator> healthIndicators = registry.getAll();
		assertThat(healthIndicators).containsKey("binder1");
		assertThat(healthIndicators.get("binder1").health().getStatus())
				.isEqualTo(Status.UP);
		assertThat(healthIndicators).containsKey("binder2");
		assertThat(healthIndicators.get("binder2").health().getStatus())
				.isEqualTo(Status.UNKNOWN);
		context.close();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void healthIndicatorsCheckWhenDisabled() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1", "binder2" },
				"spring.cloud.stream.defaultBinder:binder2",
				"management.health.binders.enabled:false", "--spring.jmx.enabled=false");

		Binder binder1 = context.getBean(BinderFactory.class).getBinder("binder1",
				MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		Binder binder2 = context.getBean(BinderFactory.class).getBinder("binder2",
				MessageChannel.class);
		assertThat(binder2).isInstanceOf(StubBinder2.class);
		try {
			context.getBean("bindersHealthIndicator", CompositeHealthIndicator.class);
			fail("The 'bindersHealthIndicator' bean should have not been defined");
		}
		catch (NoSuchBeanDefinitionException e) {
		}
		assertThat(
				context.getBean("test1HealthIndicator1", CompositeHealthIndicator.class))
						.isNotNull();
		assertThat(
				context.getBean("test2HealthIndicator2", CompositeHealthIndicator.class))
						.isNotNull();
		context.close();
	}

	@EnableAutoConfiguration
	@EnableBinding
	public static class SimpleSource {

		@Configuration
		static class TestConfig {

			@Bean
			public CompositeHealthIndicator test1HealthIndicator1() {
				return new CompositeHealthIndicator(new OrderedHealthAggregator());
			}

			@Bean
			public CompositeHealthIndicator test2HealthIndicator2() {
				return new CompositeHealthIndicator(new OrderedHealthAggregator());
			}

		}

	}

}
