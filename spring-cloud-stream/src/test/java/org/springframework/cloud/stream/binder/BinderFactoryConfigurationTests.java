/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
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
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 */
public class BinderFactoryConfigurationTests {

	@Test
	public void loadBinderTypeRegistry() throws Exception {
		try {
			ConfigurableApplicationContext context = createBinderTestContext(new String[]{});
			fail();
		}
		catch (BeanCreationException e) {
			assertThat(e.getMessage(),containsString("Cannot create binder factory, no `META-INF/spring.binders` " +
					"resources found on the classpath"));
		}
	}

	@Test
	public void loadBinderTypeRegistryWithOneBinder() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] {"binder1"});

		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry, notNullValue());
		assertThat(binderTypeRegistry.getAll().size(), equalTo(1));
		assertThat(binderTypeRegistry.getAll(), hasKey("binder1"));
		assertThat(binderTypeRegistry.get("binder1"),
				hasProperty("configurationClasses", arrayContaining(StubBinder1Configuration.class)));

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1");
		assertThat(binder1, instanceOf(StubBinder1.class));

		Binder defaultBinder = binderFactory.getBinder(null);
		assertThat(defaultBinder, is(binder1));
	}

	@Test
	public void loadBinderTypeRegistryWithOneBinderAndSharedEnvironment() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] {"binder1"}, "binder1.name=foo");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1");
		assertThat(((StubBinder1)binder1).getName(), is(equalTo("foo")));
	}

	@Test
	public void loadBinderTypeRegistryWithOneCustomBinderAndSharedEnvironment() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] {"binder1"}, "binder1.name=foo",
						"spring.cloud.stream.binders.custom.properties.foo=bar",
						"spring.cloud.stream.binders.custom.type=binder1");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("custom");
		assertThat(((StubBinder1)binder1).getName(), is(equalTo("foo")));
	}

	@Test
	public void loadBinderTypeRegistryWithOneCustomBinderAndIsolatedEnvironment() throws Exception {
		ConfigurableApplicationContext context = createBinderTestContext(
				new String[] {"binder1"}, "binder1.name=foo",
				"spring.cloud.stream.binders.custom.type=binder1",
				"spring.cloud.stream.binders.custom.environment.foo=bar",
				"spring.cloud.stream.binders.custom.inheritEnvironment=false");

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("custom");
		assertThat(((StubBinder1)binder1).getName(),isEmptyOrNullString());
	}

	@Test
	public void loadBinderTypeRegistryWithTwoBinders() throws Exception {

		ConfigurableApplicationContext context = createBinderTestContext(
				new String[]{"binder1", "binder2"});
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry, notNullValue());
		assertThat(binderTypeRegistry.getAll().size(), equalTo(2));
		assertThat(binderTypeRegistry.getAll().keySet(), containsInAnyOrder("binder1", "binder2"));
		assertThat(binderTypeRegistry.get("binder1"),
				hasProperty("configurationClasses", arrayContaining(StubBinder1Configuration.class)));
		assertThat(binderTypeRegistry.get("binder2"),
				hasProperty("configurationClasses", arrayContaining(StubBinder2ConfigurationA.class,
						StubBinder2ConfigurationB.class)));

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		try {
			binderFactory.getBinder(null);
			fail();
		}
		catch (Exception e) {
			assertThat(e, instanceOf(IllegalStateException.class));
			assertThat(e.getMessage(), containsString("A default binder has been requested, but there is more than " +
					"one binder available:"));
		}

		Binder binder1 = binderFactory.getBinder("binder1");
		assertThat(binder1, instanceOf(StubBinder1.class));
		Binder binder2 = binderFactory.getBinder("binder2");
		assertThat(binder2, instanceOf(StubBinder2.class));
	}

	@Test
	public void loadDefaultBinderWithTwoBinders() throws Exception {

		ConfigurableApplicationContext context =
				createBinderTestContext(
						new String[]{"binder1", "binder2"}, "spring.cloud.stream.defaultBinder:binder2");
		BinderTypeRegistry binderTypeRegistry = context.getBean(BinderTypeRegistry.class);
		assertThat(binderTypeRegistry, notNullValue());
		assertThat(binderTypeRegistry.getAll().size(), equalTo(2));
		assertThat(binderTypeRegistry.getAll().keySet(), containsInAnyOrder("binder1", "binder2"));
		assertThat(binderTypeRegistry.get("binder1"),
				hasProperty("configurationClasses", arrayContaining(StubBinder1Configuration.class)));
		assertThat(binderTypeRegistry.get("binder2"),
				hasProperty("configurationClasses", arrayContaining(StubBinder2ConfigurationA.class,
						StubBinder2ConfigurationB.class)));

		BinderFactory binderFactory = context.getBean(BinderFactory.class);

		Binder binder1 = binderFactory.getBinder("binder1");
		assertThat(binder1, instanceOf(StubBinder1.class));
		Binder binder2 = binderFactory.getBinder("binder2");
		assertThat(binder2, instanceOf(StubBinder2.class));

		Binder defaultBinder = binderFactory.getBinder(null);
		assertThat(defaultBinder, is(binder2));
	}
	
	public static ConfigurableApplicationContext createBinderTestContext(String[] additionalClasspathDirectories,
																		  String... properties)
			throws IOException {
		URL[] urls = ObjectUtils.isEmpty(additionalClasspathDirectories) ?
				new URL[0] : new URL[additionalClasspathDirectories.length];
		if (!ObjectUtils.isEmpty(additionalClasspathDirectories)) {
			for (int i = 0; i < additionalClasspathDirectories.length; i++) {
				urls[i] = new URL(new ClassPathResource(additionalClasspathDirectories[i]).getURL().toString() + "/");
			}
		}
		ClassLoader classLoader = new URLClassLoader(urls, BinderFactoryConfigurationTests.class.getClassLoader());
		return new SpringApplicationBuilder(SimpleApplication.class)
				.resourceLoader(new DefaultResourceLoader(classLoader))
				.properties(properties)
				.web(false)
				.run();
	}

	@Import({BinderFactoryConfiguration.class, PropertyPlaceholderAutoConfiguration.class})
	@EnableBinding
	public static class SimpleApplication {
	}
}
