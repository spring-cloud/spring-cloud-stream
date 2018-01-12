/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.List;

import javax.annotation.PostConstruct;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.PropertyAccessor;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.json.JsonPropertyAccessor;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SpelExpressionConverterConfiguration.
 *
 * @author Eric Bottard
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SpelExpressionConverterConfigurationTests.Config.class, properties = "expression: a.b")
public class SpelExpressionConverterConfigurationTests {

	@Autowired
	private Pojo pojo;

	@Autowired
	private EvaluationContext evaluationContext;

	@Autowired
	private Config config;


	@Test
	@SuppressWarnings("unchecked")
	public void converterCorrectlyInstalled() {
		Expression expression = pojo.getExpression();
		assertThat(expression.getValue("{\"a\": {\"b\": 5}}").toString()).isEqualTo("5");

		List<PropertyAccessor> propertyAccessors =
				TestUtils.getPropertyValue(this.evaluationContext, "propertyAccessors", List.class);

		assertThat(propertyAccessors).hasAtLeastOneElementOfType(JsonPropertyAccessor.class);

		propertyAccessors =
				TestUtils.getPropertyValue(this.config.evaluationContext, "propertyAccessors", List.class);

		assertThat(propertyAccessors).hasAtLeastOneElementOfType(JsonPropertyAccessor.class);
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
	public static class Config implements BeanFactoryAware {

		private BeanFactory beanFactory;

		private EvaluationContext evaluationContext;

		@Override
		public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
			this.beanFactory = beanFactory;
		}

		@Bean
		public EvaluationContext evaluationContext() {
			return ExpressionUtils.createStandardEvaluationContext(this.beanFactory);
		}

		@PostConstruct
		public void setup() {
			this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(this.beanFactory);
		}

	}

}
