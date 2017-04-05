/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
		load(MyBusEnabledConfig.class, ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED+":true");
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
		load(MyBusEnabledConfig.class, ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED+":false");
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
