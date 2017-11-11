package org.springframework.cloud.bus;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = { "spring.jmx.enabled=true", "endpoints.default.jmx.enabled=true" })
public class BusJmxEndpointTests {

	@Autowired(required = false)
	private RefreshBusEndpoint refreshBusEndpoint;

	@Autowired(required = false)
	private EnvironmentBusEndpoint environmentBusEndpoint;

	@Test
	public void contextLoads() {
		assertThat(this.refreshBusEndpoint).isNotNull();
		assertThat(this.environmentBusEndpoint).isNotNull();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	protected static class TestConfig {}
}
