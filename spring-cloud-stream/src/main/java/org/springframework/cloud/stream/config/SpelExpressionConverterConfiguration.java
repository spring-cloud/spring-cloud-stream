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

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Adds a Converter from String to SpEL Expression in the context.
 * By default, ConfigurationPropertiesBindingPostProcessor adds all converters it finds
 * in the context to its own conversionService, so this is useful for binding properties
 * of type Expression in {@literal @}ConfigurationProperties annotated classes.
 *
 * @author Eric Bottard
 */
@Configuration
public class SpelExpressionConverterConfiguration {

	@Bean
	@ConfigurationPropertiesBinding
	public Converter<String, Expression> spelConverter() {
		return new SpelConverter();
	}

	/**
	 * A simple converter from String to Expression.
	 *
	 * @author Eric Bottard
	 */
	public static class SpelConverter implements Converter<String, Expression> {

		private SpelExpressionParser parser = new SpelExpressionParser();

		@Override
		public Expression convert(String source) {
			try {
				return parser.parseExpression(source);
			}
			catch (ParseException e) {
				throw new IllegalArgumentException(String.format("Could not convert '%s' into a SpEL expression", source), e);
			}
		}
	}
}
