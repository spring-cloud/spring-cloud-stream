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

package org.springframework.cloud.stream.binding;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.ModuleChannels;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.StringUtils;

/**
 * Utility class for registering bean definitions for message channels.
 *
 * @author Marius Bogoevici
 * @author Dave Syer
 */
public abstract class BindingBeanDefinitionRegistryUtils {

	public static void registerInputChannelBeanDefinition(String qualifierValue,
			String name, String channelInterfaceBeanName,
			String channelInterfaceMethodName, BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Input.class, qualifierValue, name,
				channelInterfaceBeanName, channelInterfaceMethodName, registry);
	}

	public static void registerOutputChannelBeanDefinition(String qualifierValue,
			String name, String channelInterfaceBeanName,
			String channelInterfaceMethodName, BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Output.class, qualifierValue, name,
				channelInterfaceBeanName, channelInterfaceMethodName, registry);
	}

	private static void registerChannelBeanDefinition(
			Class<? extends Annotation> qualifier, String qualifierValue, String name,
			String channelInterfaceBeanName, String channelInterfaceMethodName,
			BeanDefinitionRegistry registry) {

		RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
		rootBeanDefinition.setFactoryBeanName(channelInterfaceBeanName);
		rootBeanDefinition.setUniqueFactoryMethodName(channelInterfaceMethodName);
		rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(qualifier,
				qualifierValue));
		registry.registerBeanDefinition(name, rootBeanDefinition);
	}

	public static void registerChannelBeanDefinitions(Class<?> type,
			final String channelInterfaceBeanName, final BeanDefinitionRegistry registry) {
		final List<String> channelNames = new ArrayList<>();
		ReflectionUtils.doWithMethods(type, new MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException,
					IllegalAccessException {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = getChannelName(input, method);
					registerInputChannelBeanDefinition(input.value(), name,
							channelInterfaceBeanName, method.getName(), registry);
				}
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = getChannelName(output, method);
					registerOutputChannelBeanDefinition(output.value(), name,
							channelInterfaceBeanName, method.getName(), registry);
				}
			}

		});
	}

	public static void registerChannelsQualifiedBeanDefinitions(Class<?> parent,
			Class<?> type, final BeanDefinitionRegistry registry) {

		if (type.isInterface()) {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
					BindableProxyFactory.class);
			rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(
					ModuleChannels.class, parent));
			rootBeanDefinition.getConstructorArgumentValues().addGenericArgumentValue(
					type);
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
		else {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(type);
			rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(
					ModuleChannels.class, parent));
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
	}

	public static String getChannelName(Annotation annotation, Method method) {
		Map<String, Object> attrs = AnnotationUtils.getAnnotationAttributes(annotation,
				false);
		if (attrs.containsKey("value")
				&& StringUtils.hasText((CharSequence) attrs.get("value"))) {
			return (String) attrs.get("value");
		}
		return method.getName();
	}

}
