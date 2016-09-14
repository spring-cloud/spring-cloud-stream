package org.springframework.cloud.bus.jackson;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.Test;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TypedRemoteApplicationEvent;
import org.springframework.cloud.bus.jackson.SubtypeModuleTests.AnotherRemoteApplicationEvent;
import org.springframework.cloud.bus.jackson.SubtypeModuleTests.MyRemoteApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;

import foo.bar.FooBarTestRemoteApplicationEvent;

public class RemoteApplicationEventScanTests {

    private BusJacksonMessageConverter converter;

    @Test
    public void importingClassMetadataPackageRegistered() {
        converter = createTestContext(DefaultConfig.class)
                .getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "org.springframework.cloud.bus.jackson",
						"org.springframework.cloud.bus.event" },
				AnotherRemoteApplicationEvent.class, MyRemoteApplicationEvent.class,
				TestRemoteApplicationEvent.class, TypedRemoteApplicationEvent.class);
    }

    @Test
    public void annotationValuePackagesRegistered() {
        converter = createTestContext(ValueConfig.class)
                .getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "foo.bar", "com.acme",
						"org.springframework.cloud.bus.event" },
				FooBarTestRemoteApplicationEvent.class, TestRemoteApplicationEvent.class,
				TypedRemoteApplicationEvent.class);
    }

    @Test
    public void annotationValueBasePackagesRegistered() {
        converter = createTestContext(BasePackagesConfig.class)
                .getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "foo.bar", "fizz.buzz", "com.acme",
						"org.springframework.cloud.bus.event" },
				FooBarTestRemoteApplicationEvent.class, TestRemoteApplicationEvent.class,
				TypedRemoteApplicationEvent.class);
    }

    @Test
    public void annotationBasePackagesRegistered() {
        converter = createTestContext(BasePackageClassesConfig.class)
                .getBean(BusJacksonMessageConverter.class);

		assertConverterBeanAfterPropertiesSet(
				new String[] { "org.springframework.cloud.bus.event.test",
						"org.springframework.cloud.bus.event" },
				TestRemoteApplicationEvent.class, TypedRemoteApplicationEvent.class);
    }

    private ConfigurableApplicationContext createTestContext(Class<?> configuration) {
        return new SpringApplicationBuilder(configuration)
                .web(false)
                .bannerMode(Banner.Mode.OFF)
                .run();
    }

	private void assertConverterBeanAfterPropertiesSet(
			final String[] expectedPackageToScan,
			final Class<?>... expectedRegisterdClasses) {
		final ObjectMapper mapper = (ObjectMapper) ReflectionTestUtils.getField(converter,
				"mapper");

		@SuppressWarnings("unchecked")
		final LinkedHashSet<NamedType> registeredSubtypes = (LinkedHashSet<NamedType>) ReflectionTestUtils
				.getField(mapper.getSubtypeResolver(), "_registeredSubtypes");

		final List<Class<?>> expectedRegisterdClassesAsList = new ArrayList<>(
				Arrays.asList(expectedRegisterdClasses));
		addStandardSpringCloudEventBusEvents(expectedRegisterdClassesAsList);

		assertTrue("Wrong RemoteApplicationEvent classes are registerd in object mapper",
				expectedRegisterdClassesAsList.size() == registeredSubtypes.size());

		for (final NamedType namedType : registeredSubtypes) {
			assertTrue(expectedRegisterdClassesAsList.contains(namedType.getType()));
		}

		assertArrayEquals("RemoteApplicationEvent packages not registered",
				(String[]) ReflectionTestUtils.getField(converter, "packagesToScan"),
				expectedPackageToScan);

	}

	private void addStandardSpringCloudEventBusEvents(
			final List<Class<?>> expectedRegisterdClassesAsList) {
		expectedRegisterdClassesAsList.add(AckRemoteApplicationEvent.class);
		expectedRegisterdClassesAsList.add(EnvironmentChangeRemoteApplicationEvent.class);
		expectedRegisterdClassesAsList.add(RefreshRemoteApplicationEvent.class);
	}

    @Configuration
    @RemoteApplicationEventScan
    static class DefaultConfig {
    }

    @Configuration
    @RemoteApplicationEventScan({"com.acme", "foo.bar"})
    static class ValueConfig {
    }

    @Configuration
    @RemoteApplicationEventScan(basePackages = {"com.acme", "foo.bar", "fizz.buzz"})
    static class BasePackagesConfig {
    }

    @Configuration
    @RemoteApplicationEventScan(basePackageClasses = TestRemoteApplicationEvent.class)
    static class BasePackageClassesConfig {
    }
}
