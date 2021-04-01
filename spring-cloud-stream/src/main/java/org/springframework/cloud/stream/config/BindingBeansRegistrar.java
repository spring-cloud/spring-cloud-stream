/*
 * Copyright 2015-2021 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindingBeanDefinitionRegistryUtils;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

/**
 * @author Marius Bogoevici
 * @author Dave Syer
 * @author Artem Bilan
 * @author Oleg Zhurakousky
 */
public class BindingBeansRegistrar implements ImportBeanDefinitionRegistrar {


	private Log logger = LogFactory.getLog(this.getClass());

	@Override
	public void registerBeanDefinitions(AnnotationMetadata metadata,
			BeanDefinitionRegistry registry) {
		AnnotationAttributes attrs = AnnotatedElementUtils.getMergedAnnotationAttributes(
				ClassUtils.resolveClassName(metadata.getClassName(), null),
				EnableBinding.class);
		try {
			for (Class<?> type : collectClasses(attrs, metadata.getClassName())) {
				if (!registry.containsBeanDefinition(type.getName())) {
					BindingBeanDefinitionRegistryUtils.registerBindingTargetBeanDefinitions(
							type, type.getName(), registry);
					BindingBeanDefinitionRegistryUtils
							.registerBindingTargetsQualifiedBeanDefinitions(ClassUtils
									.resolveClassName(metadata.getClassName(), null), type,
									registry);
				}
			}
		}
		catch (Exception e) {
			logger.warn("Failed to proxy EnableBinding annotation. If you are using functional programming style, "
					+ "ignore this warning, otherwise, annotation-based programming model is not and will "
					+ "not be suppported in native images.");
			// happens in native images, but we do not intend supporting annotation-based model for much longer
		}
	}

	private Class<?>[] collectClasses(AnnotationAttributes attrs, String className) {
		EnableBinding enableBinding = AnnotationUtils.synthesizeAnnotation(attrs,
				EnableBinding.class, ClassUtils.resolveClassName(className, null));
		return enableBinding.value();
	}
}
