/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.stub1.StubBinder1;
import org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration;
import org.springframework.cloud.stream.binder.stub2.StubBinder2;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB;
import org.springframework.cloud.stream.config.BinderFactoryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
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
 */
public class BinderFactoryConfigurationTests {

	private static ClassLoader createClassLoader(String[] additionalClasspathDirectories,
			String... properties) throws IOException {
		URL[] urls = ObjectUtils.isEmpty(additionalClasspathDirectories) ? new URL[0]
				: new URL[additionalClasspathDirectories.length];
		if (!ObjectUtils.isEmpty(additionalClasspathDirectories)) {
			for (int i = 0; i < additionalClasspathDirectories.length; i++) {
				urls[i] = new URL(new ClassPathResource(additionalClasspathDirectories[i]).getURL().toString() + "/");
			}
		}
		return new URLClassLoader(urls, BinderFactoryConfigurationTests.class.getClassLoader());
	}

	private static ConfigurableApplicationContext createBinderTestContext(String[] additionalClasspathDirectories,
			String... properties) throws IOException {
		ClassLoader classLoader = createClassLoader(additionalClasspathDirectories, properties);
		return new SpringApplicationBuilder(SimpleApplication.class)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties)
				.web(WebApplicationType.NONE)
				.run();
	}

	private static ConfigurableApplicationContext createBinderTestContextWithSources(Class<?>[] sources,
			String[] additionalClasspathDirectories,
			String... properties) throws IOException {
		ClassLoader classLoader = createClassLoader(additionalClasspathDirectories, properties);
		return new SpringApplicationBuilder(sources)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties)
				.web(WebApplicationType.NONE)
				.run();
	}

	@Test
	public void loadBinderTypeRegistryWithSelfContainedAggregatorApp() throws Exception {
		createBinderTestContextWithSources(
				new Class[] { SimpleApplication.class }, new String[] {},
				"spring.cloud.stream.internal.selfContained=true");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithOneBinder() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" });

		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll()).hasSize(1);
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
	public void loadBinderTypeRegistryWithOneBinderAndSharedEnvironment() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithOneCustomBinderAndSharedEnvironment() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] { "binder1" }, "binder1.name=foo",
				"spring.cloud.stream.binders.custom.environment.foo=bar",
				"spring.cloud.stream.binders.custom.type=binder1");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("custom", MessageChannel.class);
		assertThat(binder1).hasFieldOrPropertyWithValue("name", "foo");

		assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(binder1);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void loadBinderTypeRegistryWithTwoBinders() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(new String[] { "binder1", "binder2" });
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll()).hasSize(2);
		assertThat(binderTypeRegistry.getAll()).containsOnlyKeys("binder1", "binder2");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.containsExactly(StubBinder1Configuration.class);
		assertThat((Class[]) binderTypeRegistry.get("binder2").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder2ConfigurationA.class, StubBinder2ConfigurationB.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		try {
			binderFactory.getBinder(null, MessageChannel.class);
			fail();
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
				"spring.cloud.stream.binders.custom.inheritEnvironment=false");
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry).isNotNull();
		assertThat(binderTypeRegistry.getAll().size()).isEqualTo(1);
		assertThat(binderTypeRegistry.getAll().keySet().contains("binder1"));
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
		assertThat(binderTypeRegistry.getAll()).hasSize(2);
		assertThat(binderTypeRegistry.getAll()).containsOnlyKeys("binder1", "binder2");
		assertThat((Class[]) binderTypeRegistry.get("binder1").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder1Configuration.class);
		assertThat((Class[]) binderTypeRegistry.get("binder2").getConfigurationClasses())
				.containsExactlyInAnyOrder(StubBinder2ConfigurationA.class, StubBinder2ConfigurationB.class);

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1", MessageChannel.class);
		assertThat(binder1).isInstanceOf(StubBinder1.class);
		Binder binder2 = binderFactory.getBinder("binder2", MessageChannel.class);
		assertThat(binder2).isInstanceOf(StubBinder2.class);

		Binder defaultBinder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(defaultBinder).isSameAs(binder2);
	}

	@Import({ BinderFactoryConfiguration.class, PropertyPlaceholderAutoConfiguration.class })
	@EnableBinding
	public static class SimpleApplication {

	}
}
