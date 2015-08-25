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

package org.springframework.cloud.stream.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.ModuleChannels;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.config.DirectChannelFactoryBean;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.StringUtils;

/**
 * Utility class for registering bean definitions for message channels.
 *
 * @author Marius Bogoevici
 * @author Dave Syer
 */
public abstract class MessageChannelBeanDefinitionRegistryUtils {

	public static final String DEFAULT_INPUT_QUALIFIER_VALUE;

	public static final String DEFAULT_OUTPUT_QUALIFIER_VALUE;

	static {
		DEFAULT_INPUT_QUALIFIER_VALUE = (String) ReflectionUtils.findMethod(Input.class, "value").getDefaultValue();
		DEFAULT_OUTPUT_QUALIFIER_VALUE = (String) ReflectionUtils.findMethod(Output.class, "value").getDefaultValue();
	}

	public static void registerInputChannelBeanDefinition(String name,
			BeanDefinitionRegistry registry) {
		registerInputChannelBeanDefinition(DEFAULT_INPUT_QUALIFIER_VALUE, name, registry);
	}

	public static void registerInputChannelBeanDefinition(String qualifierValue, String name,
			BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Input.class, qualifierValue, name, registry);
	}

	public static void registerOutputChannelBeanDefinition(String name,
			BeanDefinitionRegistry registry) {
		registerOutputChannelBeanDefinition(DEFAULT_OUTPUT_QUALIFIER_VALUE, name, registry);
	}
	public static void registerOutputChannelBeanDefinition(String qualifierValue, String name,BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Output.class, qualifierValue, name, registry);
	}

	private static void registerChannelBeanDefinition(
			Class<? extends Annotation> qualifier, String qualifierValue, String name,
			BeanDefinitionRegistry registry) {

		RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
				DirectChannelFactoryBean.class);
		rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(qualifier, qualifierValue));
		registry.registerBeanDefinition(name, rootBeanDefinition);
	}

	public static List<String> registerChannelBeanDefinitions(Class<?> type,
			final BeanDefinitionRegistry registry) {
		final List<String> channelNames = new ArrayList<>();
		ReflectionUtils.doWithMethods(type, new MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException,
			IllegalAccessException {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = getName(input, method);
					registerInputChannelBeanDefinition(input.value(), name, registry);
					channelNames.add(name);
				}
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = getName(output, method);
					registerOutputChannelBeanDefinition(output.value(), name, registry);
					channelNames.add(name);
				}
			}

		});
		return channelNames;
	}

	public static void registerChannelsQualifiedBeanDefinitions(Class<?> parent, Class<?> type,
			final BeanDefinitionRegistry registry) {

		if (type.isInterface()) {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
					ChannelProxyFactory.class);
			rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(ModuleChannels.class, parent));
			rootBeanDefinition.getConstructorArgumentValues().addGenericArgumentValue(
					type);
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
		else {
			RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(type);
			rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(ModuleChannels.class, parent));
			registry.registerBeanDefinition(type.getName(), rootBeanDefinition);
		}
	}

	private static String getName(Annotation annotation, Method method) {
		Map<String, Object> attrs = AnnotationUtils.getAnnotationAttributes(annotation,
				false);
		if (attrs.containsKey("value") && StringUtils.hasText((CharSequence) attrs.get("value"))) {
			return (String) attrs.get("value");
		}
		return method.getName();
	}

	static class ChannelProxyFactory implements MethodInterceptor,
	FactoryBean<Object>, BeanFactoryAware {

		private Class<?> type;

		private Object value = null;

		private BeanFactory beanFactory;

		public ChannelProxyFactory(Class<?> type) {
			this.type = type;
		}

		@Override
		public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
			this.beanFactory = beanFactory;
		}

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			Method method = invocation.getMethod();
			if (MessageChannel.class.isAssignableFrom(method.getReturnType())) {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = getName(input, method);
					return this.beanFactory.getBean(name);
				}
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = getName(output, method);
					return this.beanFactory.getBean(name);
				}
			}
			return null;
		}

		@Override
		public Object getObject() throws Exception {
			if (this.value == null) {
				this.value = create();
			}
			return this.value;
		}

		private Object create() {
			ProxyFactory factory = new ProxyFactory(this.type, this);
			return factory.getProxy();
		}

		@Override
		public Class<?> getObjectType() {
			return this.type;
		}

		@Override
		public boolean isSingleton() {
			return true;
		}

	}

}
