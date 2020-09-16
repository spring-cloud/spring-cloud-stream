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

package org.springframework.cloud.bus;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

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
		load(MyBusEnabledConfig.class, ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED + ":true");
		assertThat(this.context.containsBean("foo")).as("missing bean from @ConditionalOnBusEnabled config").isTrue();
	}

	@Test
	public void busEnabledMissing() {
		load(MyBusEnabledConfig.class);
		assertThat(this.context.containsBean("foo")).as("missing bean from @ConditionalOnBusEnabled config").isTrue();
	}

	@Test
	public void busDisabled() {
		load(MyBusEnabledConfig.class, ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED + ":false");
		assertThat(this.context.containsBean("foo")).as("bean exists from disabled @ConditionalOnBusEnabled config")
				.isFalse();
	}

	private void load(Class<?> config, String... environment) {
		this.context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of(environment).applyTo(this.context);
		this.context.register(config);
		this.context.refresh();
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBusEnabled
	protected static class MyBusEnabledConfig {

		@Bean
		public String foo() {
			return "foo";
		}

	}

}
