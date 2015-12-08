/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotationprocessor.ImplicitExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for ImplicitExpressionConfiguration.
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {
		SpelExpressionConverterConfiguration.class,
		ImplicitExpressionConfiguration.class,
		ImplicitExpressionConfigurationTests.Config.class
})
public abstract class ImplicitExpressionConfigurationTests {

	@Autowired
	protected FooProperties fooProperties;


	@IntegrationTest("foobar.name = foo")
	public static class ImplicitExpressionSetsValueTests extends ImplicitExpressionConfigurationTests {

		@Test
		public void test() {
			Assert.assertThat(fooProperties.getNameExpression().getExpressionString(), is("'foo'"));
		}
	}

	@IntegrationTest("foobar.other = 42")
	public static class ImplicitExpressionForNonStringTypeTests extends ImplicitExpressionConfigurationTests {

		@Test
		public void test() {
			Assert.assertThat(fooProperties.getOtherExpression().getExpressionString(), is("42"));
		}
	}

	@IntegrationTest("foobar.name = foo")
	public static class ValueNotSetTests extends ImplicitExpressionConfigurationTests {

		@Test
		public void test() {
			Assert.assertThat(fooProperties.getOtherExpression(), is(nullValue()));
		}
	}

	@IntegrationTest({"foobar.name = foo", "foobar.nameExpression = foobar"})
	public static class ExplicitExpressionOverridesTests extends ImplicitExpressionConfigurationTests {

		@Test
		public void test() {
			Assert.assertThat(fooProperties.getNameExpression().getExpressionString(), is("foobar"));
		}
	}


	@Configuration
	@EnableIntegration
	@EnableConfigurationProperties(FooProperties.class)
	public static class Config {


	}


	@ConfigurationProperties(prefix = "foobar")
	public static class FooProperties {

		@ImplicitExpression(value = "the name")
		private Expression nameExpression;

		@ImplicitExpression(value = "an other thing", type = Integer.class)
		private Expression otherExpression;

		public Expression getOtherExpression() {
			return otherExpression;
		}

		public void setOtherExpression(Expression otherExpression) {
			this.otherExpression = otherExpression;
		}

		public Expression getNameExpression() {
			return nameExpression;
		}

		public void setNameExpression(Expression nameExpression) {
			this.nameExpression = nameExpression;
		}
	}

}
