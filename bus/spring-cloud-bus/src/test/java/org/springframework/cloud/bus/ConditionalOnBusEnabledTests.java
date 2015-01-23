package org.springframework.cloud.bus;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.cloud.bus.BusAutoConfiguration.SPRING_CLOUD_BUS_ENABLED;

/**
 * @author Spencer Gibb
 */
public class ConditionalOnBusEnabledTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private AnnotationConfigApplicationContext context;

	@After
	public void tearDown() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void busEnabledTrue() {
		load(MyBusEnabledConfig.class, SPRING_CLOUD_BUS_ENABLED+":true");
		assertTrue("missing bean from @ConditionalOnBusEnabled config",
				this.context.containsBean("foo"));
	}

	@Test
	public void busEnabledMissing() {
		load(MyBusEnabledConfig.class);
		assertTrue("missing bean from @ConditionalOnBusEnabled config",
				this.context.containsBean("foo"));
	}

	@Test
	public void busDisabled() {
		load(MyBusEnabledConfig.class, SPRING_CLOUD_BUS_ENABLED+":false");
		assertFalse("bean exists from disabled @ConditionalOnBusEnabled config",
				this.context.containsBean("foo"));
	}

	@Configuration
	@ConditionalOnBusEnabled
	protected static class MyBusEnabledConfig {

		@Bean
		public String foo() {
			return "foo";
		}

	}

	private void load(Class<?> config, String... environment) {
		this.context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(this.context, environment);
		this.context.register(config);
		this.context.refresh();
	}
}
