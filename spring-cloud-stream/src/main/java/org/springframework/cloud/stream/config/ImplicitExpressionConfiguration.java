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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.configurationmetadata.ConfigurationMetadataProperty;
import org.springframework.boot.configurationmetadata.ConfigurationMetadataRepositoryJsonBuilder;
import org.springframework.cloud.stream.annotationprocessor.ImplicitExpressionProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.ClassUtils;

/**
 * A configuration that installs a low level {@link org.springframework.core.env.PropertySource}
 * to support {@link org.springframework.cloud.stream.annotationprocessor.ImplicitExpression} properties.
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
@Configuration
public class ImplicitExpressionConfiguration implements EnvironmentAware, PriorityOrdered {


	private Map<String, ConfigurationMetadataProperty> allProperties;

	public ImplicitExpressionConfiguration() throws IOException {
		ConfigurationMetadataRepositoryJsonBuilder builder = ConfigurationMetadataRepositoryJsonBuilder.create();
		ResourcePatternResolver moduleResourceLoader = new PathMatchingResourcePatternResolver();
		for (Resource r : moduleResourceLoader.getResources("classpath*:" + ImplicitExpressionProcessor.METADATA_FILE)) {
			builder.withJsonResource(r.getInputStream());
		}
		allProperties = builder.build().getAllProperties();
	}

	@Override
	public void setEnvironment(final Environment environment) {
		if (environment instanceof ConfigurableEnvironment) {
			ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;

			Map<String, Object> map = new HashMap<>();
			for (Map.Entry<String, ConfigurationMetadataProperty> entry : allProperties.entrySet()) {
				String raw = environment.getProperty(entry.getKey());
				if (raw != null) {
					String type = entry.getValue().getType();
					map.put(entry.getKey() + "-expression", literalExpression(raw, type));
				}
			}
			configurableEnvironment.getPropertySources().addLast(new MapPropertySource("Implicit expressions", map));
		}
	}

	/**
	 * Return a literal SpEL expression with the given value, using proper literal notation according to the wanted
	 * type.
	 */
	private String literalExpression(Object value, String type) {
		Class<?> clazz = ClassUtils.resolveClassName(type, Thread.currentThread().getContextClassLoader());
		if (CharSequence.class.isAssignableFrom(clazz)) {
			return String.format("'%s'", value);
		}
		else if (Number.class.isAssignableFrom(clazz)) {
			return value.toString();
		}
		else {
			return value.toString();
		}
	}

	private String implicitPropertyName(String explicitPropertyName) {
		return explicitPropertyName.substring(0, explicitPropertyName.length() - "-expression".length());
	}

	@Override
	public int getOrder() {
		return PriorityOrdered.HIGHEST_PRECEDENCE;
	}
}
