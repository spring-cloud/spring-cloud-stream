/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.streams.config;

import java.lang.reflect.Field;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.streams.annotation.EnableModule;
import org.springframework.cloud.streams.annotation.Input;
import org.springframework.cloud.streams.annotation.Output;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * @author Marius Bogoevici
 */
public class ModuleBeanPostProcessor implements BeanDefinitionRegistryPostProcessor,BeanPostProcessor, ApplicationContextAware {

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		String[] beanDefinitionNames = registry.getBeanDefinitionNames();
		for (String beanDefinitionName : beanDefinitionNames) {
			BeanDefinition beanDefinition = registry.getBeanDefinition(beanDefinitionName);
			String beanClassName = beanDefinition.getBeanClassName();
			try {
				if (beanClassName != null && ClassUtils.forName(beanClassName,null).isAnnotationPresent((EnableModule.class))) {
					ReflectionUtils.doWithFields(ClassUtils.forName(beanClassName, null), new ReflectionUtils.FieldCallback() {
						@Override
						public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
							if (field.isAnnotationPresent(Input.class)) {
								RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(DirectChannelFactoryBean.class);
								rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(Input.class));
								registry.registerBeanDefinition(field.getName(), rootBeanDefinition);
							}
							if (field.isAnnotationPresent(Output.class)) {
								RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(DirectChannelFactoryBean.class);
								rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(Output.class));
								registry.registerBeanDefinition(field.getName(), rootBeanDefinition);
							}
						}
					});
				}
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		if (AnnotationUtils.findAnnotation(bean.getClass(), EnableModule.class) != null) {
			ReflectionUtils.doWithFields(bean.getClass(), new ReflectionUtils.FieldCallback() {
				@Override
				public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
					if (field.isAnnotationPresent(Output.class) || field.isAnnotationPresent(Input.class)) {
						// TODO: Do not rely on field name for injection
						ReflectionUtils.makeAccessible(field);
						ReflectionUtils.setField(field, bean, applicationContext.getBean(field.getName()));
					}
				}
			});
		}
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}
}
