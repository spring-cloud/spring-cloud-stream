package org.springframework.cloud.bus;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.RefreshListener;
import org.springframework.cloud.test.ClassPathExclusions;
import org.springframework.cloud.test.ModifiedClassPathRunner;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(ModifiedClassPathRunner.class)
@ClassPathExclusions({"spring-boot-actuator-*.jar", "spring-boot-starter-actuator-*.jar"})
public class BusAutoConfigurationClassPathTests {

    @Test
    public void refreshListenerCreatedWithoutActuator() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RefreshAutoConfiguration.class,
                        BusAutoConfiguration.class))
                .run(context -> assertThat(context)
                        .hasSingleBean(RefreshListener.class)
                        .doesNotHaveBean(RefreshBusEndpoint.class));
    }
}
