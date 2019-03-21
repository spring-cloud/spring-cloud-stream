/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class for registering bean definitions for binding targets.
 *
 * @author Marius Bogoevici
 * @author Dave Syer
 * @author Artem Bilan
 */
@SuppressWarnings("deprecation")
public abstract class BindingBeanDefinitionRegistryUtils {

	public static void registerInputBindingTargetBeanDefinition(String qualifierValue,
			String name, String bindingTargetInterfaceBeanName,
			String bindingTargetInterfaceMethodName, BeanDefinitionRegistry registry) {
		registerBindingTargetBeanDefinition(Input.class, qualifierValue, name,
				bindingTargetInterfaceBeanName, bindingTargetInterfaceMethodName,
				registry);
	}

	public static void registerOutputBindingTargetBeanDefinition(String qualifierValue,
			String name, String bindingTargetInterfaceBeanName,
			String bindingTargetInterfaceMethodName, BeanDefinitionRegistry registry) {
		registerBindingTargetBeanDefinition(Output.class, qualifierValue, name,
				bindingTargetInterfaceBeanName, bindingTargetInterfaceMethodName,
				registry);
	}

	private static void registerBindingTargetBeanDefinition(
			Class<? extends Annotation> qualifier, String qualifierValue, String name,
			String bindingTargetInterfaceBeanName,
			String bindingTargetInterfaceMethodName, BeanDefinitionRegistry registry) {

		if (registry.containsBeanDefinition(name)) {
			throw new BeanDefinitionStoreException(bindingTargetInterfaceBeanName, name,
					"bean definition with this name already exists - "
							+ registry.getBeanDefinition(name));
		}

		RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
		rootBeanDefinition.setFactoryBeanName(bindingTargetInterfaceBeanName);
		rootBeanDefinition.setUniqueFactoryMethodName(bindingTargetInterfaceMethodName);
		rootBeanDefinition
				.addQualifier(new AutowireCandidateQualifier(qualifier, qualifierValue));
		registry.registerBeanDefinition(name, rootBeanDefinition);
	}

	public static void registerBindingTargetBeanDefinitions(Class<?> type,
			final String bindingTargetInterfaceBeanName,
			final BeanDefinitionRegistry registry) {
		ReflectionUtils.doWithMethods(type, method -> {
			Input input = AnnotationUtils.findAnnotation(method, Input.class);
			if (input != null) {
				String name = getBindingTargetName(input, method);
				registerInputBindingTargetBeanDefinition(input.value(), name,
						bindingTargetInterfaceBeanName, method.getName(), registry);
			}
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = getBindingTargetName(output, method);
				registerOutputBindingTargetBeanDefinition(output.value(), name,
						bindingTargetInterfaceBeanName, method.getName(), registry);
			}
		});
	}

	public static void registerBindingTargetsQualifiedBeanDefinitions(Class<?> parent,
			Class<?> type, final BeanDefinitionRegistry registry) {

		if (type.isInterface()) {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
					BindableProxyFactory.class);
			rootBeanDefinition
					.addQualifier(new AutowireCandidateQualifier(Bindings.class, parent));
			rootBeanDefinition.getConstructorArgumentValues()
					.addGenericArgumentValue(type);
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
		else {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(type);
			rootBeanDefinition
					.addQualifier(new AutowireCandidateQualifier(Bindings.class, parent));
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
	}

	public static String getBindingTargetName(Annotation annotation, Method method) {
		Map<String, Object> attrs = AnnotationUtils.getAnnotationAttributes(annotation,
				false);
		if (attrs.containsKey("value")
				&& StringUtils.hasText((CharSequence) attrs.get("value"))) {
			return (String) attrs.get("value");
		}
		return method.getName();
	}

}
