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

package org.springframework.cloud.bus.jackson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Donovan Muller
 */
public class RemoteApplicationEventRegistrar implements ImportBeanDefinitionRegistrar {

	private static final String PACKAGES_TO_SCAN = "packagesToScan";

	private static final String BUS_JSON_CONVERTER = "busJsonConverter";

	// patterned after Spring Integration IntegrationComponentScanRegistrar

	@Override
	public void registerBeanDefinitions(final AnnotationMetadata importingClassMetadata,
			final BeanDefinitionRegistry registry) {

		Map<String, Object> componentScan = importingClassMetadata
				.getAnnotationAttributes(RemoteApplicationEventScan.class.getName(),
						false);

		Set<String> basePackages = new HashSet<>();
		for (String pkg : (String[]) componentScan.get("value")) {
			if (StringUtils.hasText(pkg)) {
				basePackages.add(pkg);
			}
		}
		for (String pkg : (String[]) componentScan.get("basePackages")) {
			if (StringUtils.hasText(pkg)) {
				basePackages.add(pkg);
			}
		}
		for (Class<?> clazz : (Class[]) componentScan.get("basePackageClasses")) {
			basePackages.add(ClassUtils.getPackageName(clazz));
		}

		if (basePackages.isEmpty()) {
			basePackages.add(
					ClassUtils.getPackageName(importingClassMetadata.getClassName()));
		}

		if (!registry.containsBeanDefinition(BUS_JSON_CONVERTER)) {
			BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder
					.genericBeanDefinition(BusJacksonMessageConverter.class);
			beanDefinitionBuilder.addPropertyValue(PACKAGES_TO_SCAN,
					basePackages.toArray(new String[basePackages.size()]));
			AbstractBeanDefinition beanDefinition = beanDefinitionBuilder
					.getBeanDefinition();

			BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition,
					BUS_JSON_CONVERTER);
			BeanDefinitionReaderUtils.registerBeanDefinition(holder, registry);
		}
		else {
			basePackages.addAll(getEarlierPackagesToScan(registry));
			registry.getBeanDefinition(BUS_JSON_CONVERTER).getPropertyValues()
					.addPropertyValue(PACKAGES_TO_SCAN,
							basePackages.toArray(new String[basePackages.size()]));
		}
	}

	private Set<String> getEarlierPackagesToScan(final BeanDefinitionRegistry registry) {
		if (registry.containsBeanDefinition(BUS_JSON_CONVERTER)
				&& registry.getBeanDefinition(BUS_JSON_CONVERTER).getPropertyValues()
						.get(PACKAGES_TO_SCAN) != null) {
			String[] earlierValues = (String[]) registry
					.getBeanDefinition(BUS_JSON_CONVERTER).getPropertyValues()
					.get(PACKAGES_TO_SCAN);
			return new HashSet<>(Arrays.asList(earlierValues));
		}

		return new HashSet<>();
	}

}
