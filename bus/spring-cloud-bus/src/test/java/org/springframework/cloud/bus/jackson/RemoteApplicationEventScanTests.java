package org.springframework.cloud.bus.jackson;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;

public class RemoteApplicationEventScanTests {

    private BusJacksonMessageConverter converter;

    @Test
    public void importingClassMetadataPackageRegistered() {
        converter = createTestContext(DefaultConfig.class)
                .getBean(BusJacksonMessageConverter.class);

        assertArrayEquals("RemoteApplicationEvent packages not registered",
                (String[]) ReflectionTestUtils.getField(converter, "packagesToScan"),
                new String[]{"org.springframework.cloud.bus.jackson",
                        "org.springframework.cloud.bus.event"});
    }

    @Test
    public void annotationValuePackagesRegistered() {
        converter = createTestContext(ValueConfig.class)
                .getBean(BusJacksonMessageConverter.class);

        assertArrayEquals("RemoteApplicationEvent packages not registered",
                (String[]) ReflectionTestUtils.getField(converter, "packagesToScan"),
                new String[]{"foo.bar", "com.acme", "org.springframework.cloud.bus.event"});
    }

    @Test
    public void annotationValueBasePackagesRegistered() {
        converter = createTestContext(BasePackagesConfig.class)
                .getBean(BusJacksonMessageConverter.class);

        assertArrayEquals("RemoteApplicationEvent packages not registered",
                (String[]) ReflectionTestUtils.getField(converter, "packagesToScan"),
                new String[]{"foo.bar", "fizz.buzz", "com.acme", "org.springframework.cloud.bus.event"});
    }

    @Test
    public void annotationBasePackagesRegistered() {
        converter = createTestContext(BasePackageClassesConfig.class)
                .getBean(BusJacksonMessageConverter.class);

        assertArrayEquals("RemoteApplicationEvent packages not registered",
                (String[]) ReflectionTestUtils.getField(converter, "packagesToScan"),
                new String[]{"org.springframework.cloud.bus.event.test",
                        "org.springframework.cloud.bus.event"});
    }

    private ConfigurableApplicationContext createTestContext(Class<?> configuration) {
        return new SpringApplicationBuilder(configuration)
                .web(false)
                .bannerMode(Banner.Mode.OFF)
                .run();
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
