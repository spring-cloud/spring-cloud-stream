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

package org.springframework.cloud.bus.jackson;

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

    // patterned after Spring Integration IntegrationComponentScanRegistrar

    @Override
    public void registerBeanDefinitions(final AnnotationMetadata importingClassMetadata,
            final BeanDefinitionRegistry registry) {

        Map<String, Object> componentScan = importingClassMetadata
                .getAnnotationAttributes(RemoteApplicationEventScan.class.getName(), false);

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
            basePackages.add(ClassUtils.getPackageName(importingClassMetadata.getClassName()));
        }

        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(BusJacksonMessageConverter.class);
        beanDefinitionBuilder.addPropertyValue("packagesToScan", basePackages.toArray(new String[basePackages.size()]));
        AbstractBeanDefinition beanDefinition = beanDefinitionBuilder.getBeanDefinition();

        BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition, "busJsonConverter");
        BeanDefinitionReaderUtils.registerBeanDefinition(holder, registry);
    }
}
