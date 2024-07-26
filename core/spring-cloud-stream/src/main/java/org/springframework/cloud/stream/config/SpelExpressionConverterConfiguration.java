/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.beans.Introspector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Role;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.config.IntegrationConverter;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.expression.SpelPropertyAccessorRegistrar;
import org.springframework.integration.json.JsonPropertyAccessor;

/**
 * Adds a Converter from String to SpEL Expression in the context.
 *
 * @author Eric Bottard
 * @author Artem Bilan
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class SpelExpressionConverterConfiguration {

	/**
	 * Provide a {@link SpelPropertyAccessorRegistrar} supplied with the
	 * {@link JsonPropertyAccessor}. This bean is used
	 * to customize an
	 * {@link org.springframework.integration.config.IntegrationEvaluationContextFactoryBean}.
	 * for additional {@link org.springframework.expression.PropertyAccessor}s.
	 * @return the SpelPropertyAccessorRegistrar bean
	 * @see org.springframework.integration.config.IntegrationEvaluationContextFactoryBean
	 */
	@Bean
	public static SpelPropertyAccessorRegistrar spelPropertyAccessorRegistrar() {
		return new SpelPropertyAccessorRegistrar()
				.add(Introspector
						.decapitalize(JsonPropertyAccessor.class.getSimpleName()),
						new JsonPropertyAccessor());
	}

	@Bean
	@ConfigurationPropertiesBinding
	@IntegrationConverter
	public Converter<Object, Expression> spelConverter(ConfigurableApplicationContext context) {
		SpelConverter converter = new SpelConverter();
		ConfigurableConversionService cs = (ConfigurableConversionService) context.getBeanFactory().getConversionService();
		if (cs != null) {
			cs.addConverter(converter);
		}
		return converter;
	}

	/**
	 * A simple converter from String to Expression.
	 *
	 * @author Eric Bottard
	 */
	public static class SpelConverter implements Converter<Object, Expression> {

		private SpelExpressionParser parser = new SpelExpressionParser();

		@Autowired
		@Qualifier(IntegrationContextUtils.INTEGRATION_EVALUATION_CONTEXT_BEAN_NAME)
		@Lazy
		private EvaluationContext evaluationContext;

		@Override
		public Expression convert(Object source) {
			try {
				if (!(source instanceof String)) {
					source = String.valueOf(source); // see https://github.com/spring-cloud/spring-cloud-stream/issues/2989
				}
				Expression expression = this.parser.parseExpression((String) source);
				if (expression instanceof SpelExpression) {
					((SpelExpression) expression)
							.setEvaluationContext(this.evaluationContext);
				}
				return expression;
			}
			catch (ParseException e) {
				throw new IllegalArgumentException(String.format(
						"Could not convert '%s' into a SpEL expression", source), e);
			}
		}

	}

}
