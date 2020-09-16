/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus.jackson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.junit.Test;
import test.foo.bar.FooBarTestRemoteApplicationEvent;

import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.UnknownRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TypedRemoteApplicationEvent;
import org.springframework.cloud.bus.jackson.SubtypeModuleTests.AnotherRemoteApplicationEvent;
import org.springframework.cloud.bus.jackson.SubtypeModuleTests.MyRemoteApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteApplicationEventScanTests {

	private BusJacksonMessageConverter converter;

	@Test
	public void importingClassMetadataPackageRegistered() {
		this.converter = createTestContext(DefaultConfig.class).getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "org.springframework.cloud.bus.jackson", "org.springframework.cloud.bus.event" },
				AnotherRemoteApplicationEvent.class, MyRemoteApplicationEvent.class, TestRemoteApplicationEvent.class,
				TypedRemoteApplicationEvent.class);
	}

	@Test
	public void annotationValuePackagesRegistered() {
		this.converter = createTestContext(ValueConfig.class).getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "test.foo.bar", "com.acme", "org.springframework.cloud.bus.event" },
				FooBarTestRemoteApplicationEvent.class, TestRemoteApplicationEvent.class,
				TypedRemoteApplicationEvent.class);
	}

	@Test
	public void annotationValueBasePackagesRegistered() {
		this.converter = createTestContext(BasePackagesConfig.class).getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "test.foo.bar", "fizz.buzz", "com.acme", "org.springframework.cloud.bus.event" },
				FooBarTestRemoteApplicationEvent.class, TestRemoteApplicationEvent.class,
				TypedRemoteApplicationEvent.class);
	}

	@Test
	public void annotationBasePackagesRegistered() {
		this.converter = createTestContext(BasePackageClassesConfig.class).getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "org.springframework.cloud.bus.event.test", "org.springframework.cloud.bus.event" },
				TestRemoteApplicationEvent.class, TypedRemoteApplicationEvent.class);
	}

	private ConfigurableApplicationContext createTestContext(Class<?> configuration) {
		return new SpringApplicationBuilder(configuration).web(WebApplicationType.NONE).bannerMode(Banner.Mode.OFF)
				.run();
	}

	private void assertConverterBeanAfterPropertiesSet(final String[] expectedPackageToScan,
			final Class<?>... expectedRegisterdClasses) {
		final ObjectMapper mapper = (ObjectMapper) ReflectionTestUtils.getField(this.converter, "mapper");

		@SuppressWarnings("unchecked")
		final LinkedHashSet<NamedType> registeredSubtypes = (LinkedHashSet<NamedType>) ReflectionTestUtils
				.getField(mapper.getSubtypeResolver(), "_registeredSubtypes");

		final List<Class<?>> expectedRegisterdClassesAsList = new ArrayList<>(Arrays.asList(expectedRegisterdClasses));
		addStandardSpringCloudEventBusEvents(expectedRegisterdClassesAsList);

		assertThat(expectedRegisterdClassesAsList.size() == registeredSubtypes.size())
				.as("Wrong RemoteApplicationEvent classes are registerd in object mapper").isTrue();

		for (final NamedType namedType : registeredSubtypes) {
			assertThat(expectedRegisterdClassesAsList.contains(namedType.getType())).isTrue();
		}

		assertThat(Arrays.asList((String[]) ReflectionTestUtils.getField(this.converter, "packagesToScan")))
				.as("RemoteApplicationEvent packages not registered").contains(expectedPackageToScan);

	}

	private void addStandardSpringCloudEventBusEvents(final List<Class<?>> expectedRegisterdClassesAsList) {
		expectedRegisterdClassesAsList.add(AckRemoteApplicationEvent.class);
		expectedRegisterdClassesAsList.add(EnvironmentChangeRemoteApplicationEvent.class);
		expectedRegisterdClassesAsList.add(RefreshRemoteApplicationEvent.class);
		expectedRegisterdClassesAsList.add(UnknownRemoteApplicationEvent.class);
	}

	@Configuration(proxyBeanMethods = false)
	@RemoteApplicationEventScan
	static class DefaultConfig {

	}

	@Configuration(proxyBeanMethods = false)
	@RemoteApplicationEventScan({ "com.acme", "test.foo.bar" })
	static class ValueConfig {

	}

	@Configuration(proxyBeanMethods = false)
	@Import(ExtraBasePackagesConfig.class)
	@RemoteApplicationEventScan(basePackages = { "com.acme", "test.foo.bar" })
	static class BasePackagesConfig {

	}

	@Configuration(proxyBeanMethods = false)
	@RemoteApplicationEventScan(basePackages = { "fizz.buzz" })
	static class ExtraBasePackagesConfig {

	}

	@Configuration(proxyBeanMethods = false)
	@RemoteApplicationEventScan(basePackageClasses = TestRemoteApplicationEvent.class)
	static class BasePackageClassesConfig {

	}

}
