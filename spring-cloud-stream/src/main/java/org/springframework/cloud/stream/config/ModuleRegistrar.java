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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.utils.MessageChannelBeanDefinitionRegistryUtils;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.MultiValueMap;

/**
 * @author Marius Bogoevici
 * @author Dave Syer
 */
@Configuration
public class ModuleRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

	public static final String SPRING_CLOUD_STREAM_BINDINGS_PREFIX = "spring.cloud.stream.bindings";

	private ConfigurableEnvironment environment;

	@Override
	public void setEnvironment(Environment environment) {
		if (environment instanceof ConfigurableEnvironment) {
			this.environment = (ConfigurableEnvironment) environment;
		}
	}

	@Override
	public void registerBeanDefinitions(AnnotationMetadata metadata,
			BeanDefinitionRegistry registry) {
		MultiValueMap<String, Object> attributes = metadata.getAllAnnotationAttributes(
				EnableModule.class.getName(), false);
		List<String> registeredChannelNames = new ArrayList<>();
		for (Class<?> type : collectClasses(attributes.get("value"))) {
			registeredChannelNames.addAll(MessageChannelBeanDefinitionRegistryUtils.registerChannelBeanDefinitions(type, registry));
			MessageChannelBeanDefinitionRegistryUtils.registerChannelsQualifiedBeanDefinitions(
					ClassUtils.resolveClassName(metadata.getClassName(), null), type,
					registry);
		}
		Properties defaultChannelNameProperties = new Properties();
		for (String registeredChannelName : registeredChannelNames) {
			defaultChannelNameProperties.put(SPRING_CLOUD_STREAM_BINDINGS_PREFIX + "." + registeredChannelName,
					"${spring.application.name:spring.cloud.stream}" + "." + registeredChannelName);
		}
		if (environment != null) {
			environment.getPropertySources().addLast(
					new PropertiesPropertySource("default-spring-cloud-stream-channel-bindings", defaultChannelNameProperties));
		}
	}

	private List<Class<?>> collectClasses(List<Object> list) {
		ArrayList<Class<?>> result = new ArrayList<Class<?>>();
		for (Object object : list) {
			for (Object value : (Object[]) object) {
				if (value instanceof Class && void.class != value) {
					result.add((Class<?>) value);
				}
			}
		}
		return result;
	}

}
