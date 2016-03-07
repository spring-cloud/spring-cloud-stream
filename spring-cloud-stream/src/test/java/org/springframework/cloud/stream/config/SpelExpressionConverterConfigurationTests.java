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
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.expression.Expression;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for SpelExpressionConverterConfiguration.
 *
 * @author Eric Bottard
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpelExpressionConverterConfigurationTests.Config.class)
@IntegrationTest("expression: a.b")
public class SpelExpressionConverterConfigurationTests {

	@Autowired
	private Pojo pojo;

	@Test
	public void converterCorrectlyInstalled() {
		assertThat(pojo.getExpression().getValue("{\"a\": {\"b\": 5}}").toString(), is((Object) "5"));
	}

	@ConfigurationProperties
	public static class Pojo {

		private Expression expression;

		public Expression getExpression() {
			return expression;
		}

		public void setExpression(Expression expression) {
			this.expression = expression;
		}
	}

	@Configuration
	@EnableBinding
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	@EnableConfigurationProperties(Pojo.class)
	public static class Config {

		/**
		 * Installs some PAs on the EvaluationContext.
		 */
		@Bean
		public static BeanPostProcessor propertyAccessorConfigurer() {
			return ChannelBindingServiceConfiguration.PostProcessorConfiguration.propertyAccessorBeanPostProcessor();
		}
	}

}
