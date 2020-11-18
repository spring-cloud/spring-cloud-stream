/*
 * Copyright 2015-2019 the original author or authors.
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

import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.stub1.StubBinder1;
import org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration;
import org.springframework.cloud.stream.binder.stub2.StubBinder2;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ObjectUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Anshul Mehra
 */
public class BinderFactoryAutoConfigurationTests {

	private static ClassLoader createClassLoader(String[] additionalClasspathDirectories,
			String... properties) throws IOException {
		URL[] urls = ObjectUtils.isEmpty(additionalClasspathDirectories) ? new URL[0]
				: new URL[additionalClasspathDirectories.length];
		if (!ObjectUtils.isEmpty(additionalClasspathDirectories)) {
			for (int i = 0; i < additionalClasspathDirectories.length; i++) {
				urls[i] = new URL(new ClassPathResource(additionalClasspathDirectories[i])
						.getURL().toString() + "/");
			}
		}
		return new URLClassLoader(urls,
				BinderFactoryAutoConfigurationTests.class.getClassLoader());
	}

	private static ConfigurableApplicationContext createBinderTestContext(
			String[] additionalClasspathDirectories, String... properties)
			throws IOException {
		ClassLoader classLoader = createClassLoader(additionalClasspathDirectories,
				properties);
		return new SpringApplicationBuilder(SimpleApplication.class)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties).web(WebApplicationType.NONE).run();
	}

	private static ConfigurableApplicationContext createBinderTestContextWithSources(
			Class<?>[] sources, String[] additionalClasspathDirectories,
			String... properties) throws IOException {
		ClassLoader classLoader = createClassLoader(additionalClasspathDirectories,
				properties);
		return new SpringApplicationBuilder(sources)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties).web(WebApplicationType.NONE).run();
	}

	@Test
	public void loadBinderTypeRegistryWithSelfContainedAggregatorApp() throws Exception {
		createBinderTestContextWithSources(new Class[] { SimpleApplication.class },
				new String[] {}, "spring.cloud.stream.internal.selfContained=true");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithOneBinder() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "spring.cloud.stream.default-binder=binder1");

		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll()).hasSize(3);
		assertThat(binderTypeRegistry.getAll()).containsKey("binder1");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder1Configuration.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);

		Binder defaultBinder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(defaultBinder).isSameAs(binder1);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithOneBinderAndSharedEnvironment()
			throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");
	}

	/*
	 * See https://github.com/spring-cloud/spring-cloud-stream/issues/1708
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithSharedEnvironmentAndServletWebApplicationType()
		throws Exception {
		String[] properties = new String[] {"binder1.name=foo", "spring.main.web-application-type=SERVLET"};
		ClassLoader classLoader = createClassLoader(new String[] { "binder1" },
			properties);
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleApplication.class, ServletWebServerFactoryAutoConfiguration.class)
			.resourceLoader(new DefaultResourceLoader(classLoader))
			.properties(properties).web(WebApplicationType.SERVLET).run();

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithOneCustomBinderAndSharedEnvironment()
			throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo",
				"spring.cloud.stream.binders.custom.environment.foo=bar",
				"spring.cloud.stream.binders.custom.environment.spring.main.sources=" + AdditionalBinderConfiguration.class.getName(),
				"spring.cloud.stream.binders.custom.type=binder1");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("custom", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");

		assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(binder1);

		SimpleApplication simpleApplication = context.getBean(SimpleApplication.class);

		assertThat(simpleApplication.binderContext).isNotNull();

		assertThat(simpleApplication.binderContext.containsBean("fooBean")).isTrue();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testCustomEnvironmentHasAccessToOuterContext() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo",
				"spring.cloud.stream.binders.custom.environment.foo=bar",
				"spring.cloud.stream.binders.custom.type=binder1");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("custom", MessageChannel.class);

		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");
		assertThat(binder1).hasFieldOrPropertyWithValue("outerContext", context);

		assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(binder1);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testStandardBinderDoesNotHaveTheOuterContextBean() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");

		assertThat(((StubBinder1) binder1).getOuterContext()).isNull();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithTwoBinders() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1", "binder2" });
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll()).hasSize(4);
		assertThat(binderTypeRegistry.getAll()).containsOnlyKeys("binder1", "binder2",
				"mock", "integration");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.containsExactly(StubBinder1Configuration.class);
		assertThat((Class[]) binderTypeRegistry.get("binder2").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder2ConfigurationA.class,
						StubBinder2ConfigurationB.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		try {
			binderFactory.getBinder(null, MessageChannel.class);
			fail("Should throw an exception");
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(IllegalStateException.class);
			assertThat(e.getMessage()).contains(
					"A default binder has been requested, but there is more than one binder available");
		}

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		Binder binder2 = binderFactory.getBinder("binder2", MessageChannel.class);
		assertThat(binder2).isInstanceOf(StubBinder2.class);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithCustomNonDefaultCandidate() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" },
				"spring.cloud.stream.binders.custom.type=binder1",
				"spring.cloud.stream.binders.custom.defaultCandidate=false",
				"spring.cloud.stream.binders.custom.inheritEnvironment=false",
				"spring.cloud.stream.default-binder=binder1");
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll().size()).isEqualTo(3);
		assertThat(binderTypeRegistry.getAll().keySet()).contains("binder1");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.contains(StubBinder1Configuration.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder defaultBinder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(defaultBinder).isInstanceOf(StubBinder1.class);
		assertThat(((StubBinder1) defaultBinder).getName()).isNullOrEmpty();

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		assertThat(binder1).isSameAs(defaultBinder);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadDefaultBinderWithTwoBinders() throws Exception {

		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1", "binder2" },
				"spring.cloud.stream.defaultBinder:binder2");
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll()).hasSize(4);
		assertThat(binderTypeRegistry.getAll()).containsOnlyKeys("binder1", "binder2",
				"mock", "integration");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder1Configuration.class);
		assertThat((Class[]) binderTypeRegistry.get("binder2").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder2ConfigurationA.class,
						StubBinder2ConfigurationB.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		assertThat(((StubBinder1) binder1).getFromCustomization()).isEqualTo("customizer-applied-binder1");
		Binder binder2 = binderFactory.getBinder("binder2", MessageChannel.class);
		assertThat(binder2).isInstanceOf(StubBinder2.class);
		assertThat(((StubBinder2) binder2).getFromCustomization()).isEqualTo("customizer-applied-binder2");

		Binder defaultBinder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(defaultBinder).isSameAs(binder2);
	}

	@Import({ BinderFactoryAutoConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class,
			BindingServiceConfiguration.class })
	@EnableBinding
	public static class SimpleApplication {

		private volatile ApplicationContext binderContext;

		@Bean
		public DefaultBinderFactory.Listener testBinderListener() {
			return (configurationName, binderContext) -> {
				this.binderContext = binderContext;
			};

		}

		@Bean
		public BinderCustomizer binderCustomizer() {
			return binder -> {
				if (binder instanceof StubBinder1) {
					((StubBinder1) binder).setFromCustomization("customizer-applied-binder1");
				}
				else if (binder instanceof StubBinder2) {
					((StubBinder2) binder).setFromCustomization("customizer-applied-binder2");
				}
			};
		}
	}

	@Configuration
	public static class AdditionalBinderConfiguration {

		@Bean
		public String fooBean() {
			return "foo";
		}
	}

}
